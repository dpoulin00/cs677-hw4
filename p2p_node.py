import copy
import json
import pickle
import random
import socket
import select
import time
import threading
from typing import Dict
import uuid
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import timedelta
from datetime import datetime
from enum import Enum
from pathlib import Path
import pandas as pd
from pandas.core.interchange.dataframe_protocol import DataFrame


class Role(Enum):
    """Defines types of nodes."""
    BUYER = 0
    SELLER = 1
    BUYER_AND_SELLER = 2

class Item(Enum):
    """Defines types of nodes."""
    SALT = 0
    FISH = 1
    BOAR = 2

class BuyMsgType(Enum):
    """Defines buy msg types."""
    INIT = 0  # Buyer sends to leader to start buy process (multicast, increment clock)
    RESPONSE = 1  # Leader responds to buyer (unicast)
    PAYMENT = 2  # Leader pays seller (unicast)
    RESTOCK = 3  # Seller sends to leader to stock new inventory (unicast, increment clock)

class ElecMsgType(Enum):
    """Defines election msg types."""
    RESIGN = 0  # Leader multicasts to all nodes to resign (multicast, increment clock)
    ELECT = 1  # Node tries to elect self. (multicast to upstream nodes)
    OKAY = 2  # Response to ELECT when we have higher ID. (unicast)
    IWON = 3  # If nobody responds to ELECT, we're the new leader. (multicast)

class ControlMsgType(Enum):
    """
    Defines control message types.
    Mostly for communication with parent process, except ACK, which is for ACKing transactions.
    """
    STOP = 0  # Parent process sends to shuts down the node
    # Below msgs could be used to include stop conditions for network
    REPORT_CMD = 1  # Parent process requests status from nodes
    REPORT = 2  # Node sends status to parent process
    ACK = 3  # Leader sends this back to each node after certain msgs. A lack of this tells node leader resigned.

class ActionType(Enum):
    """Define action types (for log and msgs)"""
    ELECT = 0
    BUY = 1
    RESTOCK = 2

class ActionStatus(Enum):
    """
    Define action statuses (from client perspective).
    Used in node logs, let's us know what's started, acked, done, and what needs resending.
    """
    STARTED = 0  # Action started, but not acked by leader
    ACKED = 1  # Action acked by leader
    DONE = 2  # Action finished.
    NEEDS_RESEND = 3  # Action needs to be resent

class ActionProcessStatus(Enum):
    """
    Defines action status (from leader perspective).
    Let's us know what transactions still need to be processed.
    """
    RECIEVED = 0  # Action recieved, not done yet
    DONE = 1  # Action done


class P2PNode:
    def __init__(self, id: int, port_number: int, is_buyer: bool, is_seller: bool,
                 nodes: Dict[int, int],  # keys are IDs, vals are ports
                 shopping_list: list[Dict] | list=[],
                 selling_list: list[Dict] | list=[]):
        """
        Initializes node by recording whether eacah is a buyer or a seller,
        and a list of neighbors (all nodes in network). Also sets up
        some data structures we'll need.
        """
        # Unique to each node
        self.id = id
        self.port_number = port_number
        self.server_socket = socket.socket()
        self.node_log = pd.DataFrame(columns=["uid", "timestamp", "clock", "type", "item", "quantity", "status"])
        # Attributes used by all nodes
        self.num_sent_msgs = 0
        self.num_accepted_msgs = 0
        self.nodes = nodes
        node_keys = list(nodes.keys())
        node_keys.append(self.id)
        node_keys.sort()
        self.clock = {id:0 for id in node_keys}
        self.is_buyer = is_buyer
        self.is_seller = is_seller
        self.is_leader = False
        self.leader = None
        self.running = False  # Set to true and false by parent process
        # Attributes used by sellers
        self.next_restock_ts = datetime.now() + timedelta(0,random.randint(12,15))  # days, seconds
        self.revenue = 0
        self.prices = {
            "SALT": 1,
            "FISH": 5,
            "BOAR": 10
        }
        # Attributes used by buyers
        self.next_buy_ts = datetime.now() + timedelta(0,random.randint(1,3))  # days, seconds
        # Attributes use by leaders and for elections
        self.resigned = False  # Set to true temporarily when we resign
        self.leader_log = pd.DataFrame(columns=["uid", "timestamp", "clock", "sender", "type", "item", "quantity", "status"])
        self.elections = dict()  # keys are UIDs, values are timestamps
        self.next_resign_ts = None
        self.last_election_ts = None
        self.leader_log_path = Path(r"leader_log.csv")
        # Optional attributes used for testing
        self.shopping_list = shopping_list
        self.selling_list = selling_list
        # Locks
        self.is_leader_lock = None  # Used when sending acks to make sure acked transaction gets into log
        self.node_log_lock = None
        self.leader_log_lock = None
        self.clock_lock = None # Used to update the clock when a new request is made
        
    
    def start(self):
        """
        Called by parent process to start node running.
        Sets self.running to True, sets up socket, and starts run loop
        Since we only have one loop, no need to spawn a thread for run loop.
        """
        # Set up server socket
        self.server_socket = socket.socket()
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.settimeout(100)  # Time out so we can gracefully exit once we stop seeing messages.
        self.server_socket.bind((socket.gethostname(), self.port_number))
        self.server_socket.listen(10000)
        # Set up locks
        self.is_leader_lock = threading.Lock()
        self.node_log_lock = threading.Lock()
        self.leader_log_lock = threading.Lock()
        self.clock_lock = threading.Lock()
        # Start run loop
        self.running = True
        print(f"{datetime.now()}, status, node {self.id} starting using port {self.port_number}")
        self.run_loop()
        return
    
    def stop(self):
        """
        Called when we receive stop message from parent process.
        Sets self.running to False, which will end our run loop.
        Closes server socket.
        """
        print(f"{datetime.now()}, status, node {self.id} stopping")
        self.running = False
        self.server_socket.close()
        return
    
    def run_loop(self):
        """
        Called when node starts running. 
        Runs a loop that does the following:
        1. Check for and handle all incoming messages. Spawn thread to handle each.
        If no leader exists:
        2. Enter election logic.
        If some other node is the leader, it also does the following:
        3. Check if it's time to send a buy request: if so, spawn thread to handle it.
        4. Check if it's time to stock an item; if so, spawn thread to handle it.
        If this node is the leader, it also does the following:
        5. Check if it's time to resign; if so, spawn thread to handle it.
        """
        # Open thread executor, and enter listening loop
        with ThreadPoolExecutor(max_workers=100) as executor:
            while self.running:
                # Accept msgs. If a STOP shows up, we need to break out of loop.
                stopped = self.accept_msgs(executor=executor)
                if stopped:
                    break
                elif self.leader is None:
                    # If there's no leader (either bc we just initialized or bc a buy or restock msg went unacked),
                    # we're in the election logic. If we haven't started an election in a while,
                    # start a new one. Otherwise, wait to get an IWON
                    ongoing_election_ts = self.get_most_recent_election(status=ActionStatus.STARTED.name)
                    last_election_ts = self.get_most_recent_election(status=ActionStatus.DONE.name)
                    if ongoing_election_ts is None:
                        # If no ongoing election, and it's been a while since the last election (or
                        # there was no last election, i.e. we just initialized), start
                        # a new one. Otherwise, wait.
                        if last_election_ts is None or last_election_ts + timedelta(0, 30) < datetime.now():
                            self.elect()
                        else:
                            time.sleep(1)  # sleep to avoid spamming through loop
                    elif ongoing_election_ts is not None:
                        # if there is an ongoing election, and it's been a while, declare victory.
                        # Otherwise, keep waiting.
                        if ongoing_election_ts + timedelta(0, 10) < datetime.now():
                            self.iwon()
                        else:
                            time.sleep(1)
                elif not self.is_leader:
                    # We have a leader but we're not it. Buy and sell items.
                    # We only buy and sell after certain intervals.
                    if self.is_seller:
                        if datetime.now() > self.next_restock_ts:
                            self.restock()
                    if self.is_buyer:
                        if datetime.now() > self.next_buy_ts:
                            self.buy()
                    if True:
                        # Check if any entries have lingered too long (which we'll take to mean
                        # the leader went down), or if any need to be resent
                        self.review_node_log()
                elif self.is_leader:
                    # If we are the leader, check if we've caught up to any pending
                    # transcations in the leader log. If so, handle these.
                    # Also check if it's been long enough that we need to resign.
                    # When we come back after resigning, start a new election.
                    # FIXME: make time between winning election and resigning random
                    # by choosing a random time delta at time of winning.
                    # FIXME: make a separate thread to review the leader log to allow for concurrency
                    # My concern with doing so is the leader changing mid execution of the process
                    # although I suppose that's what the ACK system is in place to handle
                    self.review_leader_log()
                    last_election_ts = self.get_most_recent_election(status=ActionStatus.DONE.name)
                    next_resign_ts = last_election_ts + timedelta(0, 60)  # days, seconds
                    if datetime.now() > next_resign_ts:
                        self.resign(sleep=True)
                        self.elect()
        return
    
    def accept_msgs(self, executor) -> bool:
        """
        Called by run_loop to iterate through socket messages and parse each.
        Returned bool indicates whether we received the STOP message.
        If returning True, we did receive a stop message.
        """
        stopped = False
        # Check if there's a new message. If so, handle it
        while select.select([self.server_socket], [], [], 0.1)[0]:
            self.num_accepted_msgs += 1
            socket_connection, addr = self.server_socket.accept()
            data = socket_connection.recv(4096)
            socket_connection.close()
            try:
                msg = pickle.loads(data)
            except Exception as e:
                # Helpful for debugging multiple threads, processes
                print(f"Pickle exception:")
                print(e)
            # If the msg isn't a STOP, spawn a thread. But if
            # it is a stop, we handle here, such that we
            # can return False (and thus exit the run_loop)
            if msg["type"] != ControlMsgType.STOP.name:
                executor.submit(self.handle_msg, msg, addr)
            else:
                self.stop()
                stopped = True
                break
        return stopped
    
    def handle_msg(self, msg, addr):
        """
        Unpickles msg, checks type, and calls corresponding function to handle it.
        """
        # Parse message type and call the corresponding function
        match msg["type"]:
            case BuyMsgType.INIT.name:
                if self.is_leader:
                    self.append_to_leader_log(msg, transaction_type=ActionType.BUY)
                    self.send_ack(uid=msg["uid"], dest=msg["sender"])
            case BuyMsgType.RESTOCK.name:
                if self.is_leader:
                    self.append_to_leader_log(msg, transaction_type=ActionType.BUY)
                    self.send_ack(uid=msg["uid"], dest=msg["sender"])
            case BuyMsgType.PAYMENT.name:
                pass
            case BuyMsgType.RESPONSE.name:
                pass
            case ControlMsgType.ACK.name:
                self.recieve_ack(msg)
            case ElecMsgType.RESIGN.name:  # TODO: REMOVE. THIS CASE NO LONGER USED
                if self.leader == msg["sender"]:
                    self.leader = None
                self.elect()
            case ElecMsgType.ELECT.name:
                if self.id > msg["sender"]:
                    self.okay(msg)
                    self.elect()
            case ElecMsgType.OKAY.name:
                self.im_okay(uid=msg["uid"])
            case ElecMsgType.IWON.name:
                self.youwon(new_leader = msg["sender"])
            case ControlMsgType.STOP.name:
                self.stop()
        return
    
    def send_msg(self, msg, dest:int):
        """
        Called anytime we need to send a msg.
        Send message to node whose ID is dest.
        """
        self.num_sent_msgs += 1
        #print(self.num_sent_msgs)
        port = self.nodes[dest]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as node_socket:
            node_socket.connect((socket.gethostname(), port))
            serialized_msg = pickle.dumps(msg, -1)  # -1 is used to pick best representation
            node_socket.sendall(serialized_msg)
        return

    def resend_msg(self, row: DataFrame):
        """
        Called anytime we need to resend a message.
        The row containing the message information is present.
        """

        if row["type"] == BuyMsgType.INIT.name:
            self.node_log_lock.acquire_lock()
            msg = dict(
                uid = row["uid"],
                sender = self.id,
                clock = row["clock"],
                type = BuyMsgType.INIT.name,
                item = row["item"],
                quantity = row["quantity"],
            )
            self.node_log.loc[self.node_log["uid"] == row["uid"], "status"] = ActionStatus.STARTED.name
            self.node_log_lock.release()
            self.send_msg(msg, self.leader)
    
    def send_ack(self, uid, dest):
        """
        Whenever the leader gets a BY or RESTOCK msgs, the leader adds them to the 
        leader log and then calls this, such that the senders know their transcations are in the
        log.
        """
        msg = dict(
            type = ControlMsgType.ACK.name,
            uid=uid,
            sender=self.id
        )
        # After picking up lock, make a final check here.
        # If we're still the leader, our adding this
        # request to the log would be included in any saved logs, and thus it's
        # safe to send the ack. If we're no longer the leader, we may or
        # may not have gotten the request into the log in time. Since we're
        # note sure, don't send an ACK.
        # The node will need to resend it; leaders will thus need to 
        # be able to figure out if they're getting a request they already have.
        self.is_leader_lock.acquire_lock()
        if self.is_leader:
            self.send_msg(msg=msg, dest=dest)
            print(f"{datetime.now()}, {uid}, node {self.id} sent ack")
        self.is_leader_lock.release_lock()
        return
    
    def recieve_ack(self, msg):
        """
        When leader sends an initial ACK back, mark transaction as ACKED in the log.
        """
        uid = msg["uid"]
        print(f"{datetime.now()}, {uid}, node {self.id} received ack")
        self.update_node_log(uid=uid, timestamp=datetime.now(),
                             status=ActionStatus.ACKED.name)
        return

    def buy(self):
        """
        Initiate buy request by selecting random item and messaging leader.
        """
        # Initialize buy request
        uid = uuid.uuid4()
        if len(self.shopping_list) == 0:
            item = random.choice(list(Item)).name
            quantity = random.choice(range(1, 10))
        else:
            item_quantity_dict = self.shopping_list.pop()
            item = list(item_quantity_dict.keys())[0]
            quantity = item_quantity_dict[item]

        # Add to log
        self.clock_lock.acquire()
        self.update_vector_clock_local_event()
        self.append_to_node_log(uid=uid, timestamp=datetime.now(), clock=copy.deepcopy(self.clock),
                                type=BuyMsgType.INIT.name, item=item, quantity=quantity,
                                status=ActionStatus.STARTED.name)

        print(f"{datetime.now()}, buy, node {self.id} is buying {item}")
        # Send out request
        msg = dict(
            uid = uid,
            sender = self.id,
            clock = self.clock,
            type = BuyMsgType.INIT.name,
            item = item,
            quantity = quantity
        )
        self.clock_lock.release()
        for nid in self.nodes.keys():
            self.send_msg(msg=msg, dest=nid)
        self.next_buy_ts = datetime.now() + timedelta(0, 10)
        return
    
    def finalize_buy(self):
        """
        called when trader sends back message confirming the buy went through.
        """
        return
    
    def restock(self):
        """
        Seller picks new item, stocks a certain amount of it, and sends message to leader indicating this.
        """
        return
    
    def get_paid(self, price:int):
        """
        After trader finalizes sale, process payment from trader.
        To
        """
        return

    def review_node_log(self):
        """
        Called by each non-leader node during the run_loop.
        If any items in the node log have status NEEDS_RESEND, resend them.
        If transactions have gone unACKED for too long,
        set self.leader to None to trigger election and set their status to NEEDS_RESEND.
        """
        # FIXME: First, check the STARTED transactions to see if they've
        # lingered too long. This will save the resending of msgs when the
        # leader is down.
        log = self.node_log.copy()
        log = log[log["status"] != ActionStatus.DONE.name]
        for i, row in log.iterrows():
            uid = row["uid"]
            status = row["status"]
            timestamp = row["timestamp"]
            if status == ActionStatus.NEEDS_RESEND.name:
                # FIXME: implement resending
                # Currently updated to retry initializing a buy request, will need to be updated for each request
                # type as they show up
                # also need to change message status in log and update timestamp
                self.resend_msg(row)
                test = 2
            elif status == ActionStatus.STARTED.name:
                # Check if action has gone unacked for too long.
                # If so, set leader to False and break out of
                # loop. This will lead to an election
                if timestamp + timedelta(0, 30) < datetime.now():
                    self.leader = None
                    self.node_log_lock.acquire_lock()
                    self.node_log.loc[self.node_log["status"] == ActionStatus.STARTED.name,
                                      "status"] = ActionStatus.NEEDS_RESEND.name
                    self.node_log_lock.release_lock()
                    break
        return
    
    def review_leader_log(self):
        """
        Called in each run_loop by the leader.
        Check if we've caught up to the clock in any transactions still in the log.
        If so, we can process those transactions.
        """
        log = self.leader_log.copy()
        log = log[(log["status"] != ActionStatus.DONE.name) & (log["status"] != ActionStatus.NEEDS_RESEND.name)]
        self.clock_lock.acquire()
        for i, row in log.iterrows():
            # hack-y fix to get around datatype problems when reading from CSV, should probably be done differently
            if type(row["clock"]) is str:
                row["clock"] = {int(key): int(val) for key, val in [item.split(': ') for item in row["clock"][1:-1].split(', ')]}

            valid_clock_diff = self.verify_clock_valid(row["clock"], row["sender"])
            if valid_clock_diff is True:
                # process request
                # FIXME: add request processing code here

                # FIXME: remove temporary update of status here and implement it in request logic
                self.leader_log_lock.acquire_lock()
                self.leader_log.loc[self.leader_log["uid"] == row["uid"], "status"] = ActionStatus.DONE.name
                self.leader_log_lock.release_lock()
                # update clock
                self.update_vector_clock_received_message(row["clock"], row["sender"])
                # set like this to only process one request per iteration of loop, although this may not
                # be necessary and can probably be removed
                break

        self.clock_lock.release()
        return

    def verify_clock_valid(self, clock: dict, sender: int):
        try:
            return_bool = True
            for key in clock.keys():
                node_clock_val = self.clock[key]
                other_node_clock = clock[key]
                if key == sender:
                    return_bool = return_bool and other_node_clock == node_clock_val + 1
                else:
                    return_bool = return_bool and other_node_clock <= node_clock_val
            return return_bool
        except:
            test = 2

    def resign(self, sleep:bool):
        """
        Called when leader resigns.
        Saves leader log.
        Sets self.is_leader to False.
        If sleep is True, we also wait for a while here to simulate a node
        being down (a leader calling in sick). While down, we keep
        the queue empty.
        """
        # Pick up log lock, and save log. Then delete log from node. Make sure to
        # set is_leader to False before releasing lock, so that our message handling knows not to send an ACK
        print(f"{datetime.now()}, election, node {self.id} is resigning")
        self.is_leader_lock.acquire_lock()
        self.is_leader = False
        self.leader_log_lock.acquire_lock()
        self.leader_log.to_csv(self.leader_log_path, index=False)
        self.leader_log = self.leader_log.drop(self.leader_log.index)  # wipe out leader log
        self.leader_log_lock.release_lock()
        self.is_leader_lock.release_lock()
        # Go offline for wait_interval seconds. Keep socket queue clear while offline.
        if sleep:
            wait_interval = random.choice(range(35, 65))
            wait_time = 0
            while wait_time < wait_interval:
                while select.select([self.server_socket], [], [], 0.1)[0]:
                    socket_connection, addr = self.server_socket.accept()
                    socket_connection.recv(4096)
                time.sleep(1)
                wait_time += 1
            print(f"{datetime.now()}, node, node {self.id} is back online")
        return
    
    def elect(self):
        """
        Called when leader goes down, this node comes back online after resigning,
        or an elect msg is received.
        Sends elect message to all nodes with higher ID than this node.
        If any response, we wait for new leader.
        If no response, send iwon message.
        Note that we won't start a new election if we already have one open.
        Note also that we track elections in the node_log.
        """
        # If we haven't started an election, do so now
        if self.get_most_recent_election(status=ActionStatus.STARTED.name) is not None:
            pass
        else:
            print(f"{datetime.now()}, election, node {self.id} is starting election")
            # Add election to node log
            uid = uuid.uuid4()
            self.append_to_node_log(type=ActionType.ELECT.name,
                                    uid = uid,
                                    timestamp = datetime.now(),
                                    status = ActionStatus.STARTED.name,
                                    clock = dict(),
                                    item=None,
                                    quantity=None)
            # If we have the max ID, we win by default.
            if self.id > max(self.nodes.keys()):
                self.iwon()
            # Otherwise, send out ELECT msgs to upstream nodes
            else:
                msg = dict(
                    type = ElecMsgType.ELECT.name,
                    sender = self.id,
                    uid = uid
                )
                for nid in self.nodes.keys():
                    if nid > self.id:
                        self.send_msg(msg=msg, dest=nid)
        return
    
    def okay(self, incoming_msg):
        """
        Answers an elect message if this node has higher ID than elector.
        """
        msg = dict(
            type = ElecMsgType.OKAY.name,
            uid = incoming_msg["uid"]
        )
        self.send_msg(msg=msg, dest=incoming_msg["sender"])
        return
    
    def im_okay(self, uid):
        """
        Called when we recieve an OKAY msg.
        Mark corresponding election uid as done.
        If this node is the leader, we resign.
        """
        # Record election as finished
        self.update_node_log(uid=uid, status=ActionStatus.DONE.name, timestamp=datetime.now())
        # If currently the leader, we must now resign
        if self.is_leader:
            self.resign(sleep=False)
        return
    
    def iwon(self):
        """
        Send out an iwon message if no node responds to our elect message.
        This is also where we set ourselves as the leader, including setting
        is_leader to True, setting leader to this node, and picking up the leader log.
        Note we have to wait to pick up the leader log, such that the previous leader
        has time to save it.
        """
        # We'll only pick up the log from the disk if we weren't already the leader
        already_leader = self.is_leader
        # Send out IWON msgs
        msg = dict(
            type = ElecMsgType.IWON.name,
            sender = self.id,
        )
        for nid in self.nodes.keys():
            self.send_msg(msg=msg, dest=nid)
        # Set node to leader
        self.leader = self.id
        self.is_leader = True
        print(f"{datetime.now()}, election, node {self.id} won election")
        # Record all ongoing elections as done in node log
        self.node_log_lock.acquire_lock()
        election_mask = (self.node_log["type"] == ActionType.ELECT.name)
        started_mask = (self.node_log["status"] == ActionStatus.STARTED.name)
        self.node_log.loc[election_mask & started_mask, "status"] = ActionStatus.DONE.name
        self.node_log.loc[election_mask & started_mask, "timestamp"] = datetime.now()
        # Wait a few seconds, then pick up the log. This gives old leader time to save it.
        time.sleep(20)
        if not already_leader:
            self.leader_log_lock.acquire_lock()
            self.leader_log = self.leader_log.drop(self.leader_log.index)
            if self.leader_log_path.exists():
                self.leader_log = pd.read_csv(self.leader_log_path)
            #print(self.leader_log)
            self.leader_log_lock.release_lock()
        self.node_log_lock.release_lock()
        return
    
    def youwon(self, new_leader):
        """
        Called when we receive an IWON message.
        If this node is the leader, resign (which will save the leader log).
        Marks all ongoing elections as done.
        """
        if self.is_leader:
            self.resign(sleep=False)
        self.leader = new_leader
        # Mark all ongoing elections as done
        self.node_log_lock.acquire_lock()
        election_mask = (self.node_log["type"] == ActionType.ELECT.name)
        started_mask = (self.node_log["status"] == ActionStatus.STARTED.name)
        self.node_log.loc[election_mask & started_mask, "status"] = ActionStatus.DONE.name
        self.node_log.loc[election_mask & started_mask, "timestamp"] = datetime.now()
        self.node_log_lock.release_lock()
        return

    def get_most_recent_election(self, status:str):
        """
        Get most recent election timestamp. Status tells us
        if we're looking for DONE or STARTED elections.
        Used to figure out when we've won an election (and thus need to send an IWON)
        and when the last finished election was (so if it was a long time ago, and there's
        still no leader, we can start a new one).
        """
        df = self.node_log.copy()
        elect_mask = (df["type"]==ActionType.ELECT.name)
        status_mask = (df["status"]==status)
        past_elections = df[ elect_mask & status_mask]
        if len(past_elections) == 0:
            last_election = None
        else:
            last_election = past_elections["timestamp"].max()
        return last_election
    
    def append_to_node_log(self, uid, timestamp, clock: dict, type: str, status: str,
                           item: str | None, quantity: int | None):
        """
        Add entry to node_log.
        """
        # FIXME: add log lock
        self.node_log_lock.acquire_lock()
        log_entry = dict(
            uid = uid,
            timestamp = timestamp,
            clock = clock,
            type = type,
            item = item,
            quantity = quantity,
            status = status
        )
        self.node_log.loc[len(self.node_log)] = log_entry
        self.node_log_lock.release_lock()
        return


    def update_node_log(self, uid, timestamp, status: str):
        """
        Update this UID's timestamp and status in node log.
        """
        self.node_log_lock.acquire_lock()
        curr_status = self.node_log[self.node_log["uid"] == uid]["status"].iloc[0]
        if curr_status != ActionStatus.DONE.name:
            self.node_log.loc[self.node_log["uid"] == uid, "status"] = status
            self.node_log.loc[self.node_log["uid"] == uid, "timestamp"] = timestamp
        self.node_log_lock.release_lock()
        return
    
    def append_to_leader_log(self, msg, transaction_type):
        """
        Append transaction to leader log. If we already have this UID in the log,
        don't add it.
        """
        log_entry = dict(
            uid = msg["uid"],
            timestamp = datetime.now(),
            clock = msg["clock"],
            sender = msg["sender"],
            type = transaction_type,
            item = msg["item"],
            quantity = msg["quantity"],
            status = ActionProcessStatus.RECIEVED.name,

        )
        self.leader_log_lock.acquire_lock()
        if msg["uid"] not in self.leader_log["uid"].to_list():
            self.leader_log.loc[len(self.leader_log)] = log_entry
        self.leader_log_lock.release_lock()
        return
    
    def update_leader_log(self, uid, timestamp, status: str):
        """
        Update this UID's entry in node log.
        """
        self.leader_log_lock.acquire_lock()
        curr_status = self.leader_log[self.leader_log["uid"] == uid]["status"].iloc[0]
        if curr_status != ActionStatus.DONE.name:
            self.leader_log.loc[self.leader_log["uid"] == uid, "status"] = status
            self.leader_log.loc[self.leader_log["uid"] == uid, "timestamp"] = timestamp
        self.leader_log_lock.release_lock()
        return

    def update_vector_clock_local_event(self):
        """
        Performs the update to the local clock, to be called when a local event occurs.
        """
        self.clock[self.id] += 1

    def update_vector_clock_received_message(self, received_clock: dict, sender_id: int):
        """
        Performs the update to the local clock, to be called when a message updating the clock is received.
        Note: this function is only to be called when
        """
        self.clock[sender_id] = self.clock[sender_id] + 1






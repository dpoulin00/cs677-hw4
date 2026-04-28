import copy
import json
import math
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
import numpy as np
import pandas as pd
from pandas.core.interchange.dataframe_protocol import DataFrame
#import debugpy

# Make sure threads fail loudly
def custom_hook(args):
    print(f"Thread failed: {args.exc_type.__name__}: {args.exc_value}")
threading.excepthook = custom_hook

# We define several class that inherit enums. These are used throughout
# the program to define message types, transaction statuses, etc.
# While we only ever use the .name atrribute (which is just a string),
# this still helps us avoid typos in strings.
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

class MsgType(Enum):
    """Defines msg types."""
    BUY = 0  # Msg to trader to warehouse requesting to purchase item.
    RESTOCK = 1  # Msg to trader to warehouse requesting to sell item.
    BUY_REPLY = 2  # Msg from warehouse to trader to node, accepting or denying purchase.
    RESTOCK_REPLY = 3  # Msg from warehouse to trader to node, accepting or denying sell.
    UPDATE = 4  # Msg from trader to warehouse sending an update to inventory.
    UPDATE_REPLY = 5  # Msg from warehouse to trader indicating if update was accepted.

class ElecMsgType(Enum):
    """Defines election msg types."""
    ELECT = 0  # Node tries to elect self. (multicast to upstream nodes)
    OKAY = 1  # Response to ELECT when we have higher ID. (unicast)
    IWON = 2  # If nobody responds to ELECT, we're the new leader. (multicast)

class ControlMsgType(Enum):
    """
    Defines control message types.
    Mostly for communication with parent process, except ACK, which is for ACKing transactions.
    """
    STOP = 0  # Parent process sends to shuts down the node

class ActionStatus(Enum):
    """
    Define action statuses (from client perspective).
    Used in node logs, let's us know what's started, acked, done, and what needs resending.
    """
    STARTED = 0  # Action started, but not acked by leader
    DONE = 1  # Action finished.

class P2PNode:
    def __init__(self, id: int, port_number: int, is_buyer: bool, is_seller: bool,
                 nodes: Dict[int, int],  # keys are IDs, vals are ports
                 shopping_list: list[Dict] | list=[],
                 selling_list: list[Dict] | list=[],
                 max_requests:int = -1):
        """
        Initializes node by recording whether each node is a buyer or a seller,
        and a list of neighbors (all nodes in network). Also sets up
        some data structures we'll need.
        """
        # Fixed details about this node
        self.id = id
        self.port_number = port_number
        self.server_socket = socket.socket()
        self.node_log = pd.DataFrame(columns=["uid", "timestamp", "clock", "type", "item", "quantity", "status", "init_time", "acked_time", "received_time"])
        # Attributes used by all nodes
        self.num_sent_msgs = 0
        self.num_accepted_msgs = 0
        self.nodes = nodes
        node_keys = list(nodes.keys())  # Used to iterate through vector clock.
        node_keys.append(self.id)
        node_keys.sort()
        self.clock = {id:0 for id in node_keys}
        self.leader_clock = {id:0 for id in node_keys}
        self.is_buyer = is_buyer
        self.is_seller = is_seller
        self.stopped = False
        self.is_leader = False
        self.leader = None  # Used to determine who the leader if. If None, we'll assume there is no leader.
        self.running = False  # Set to true on startup, set to false on receiving a STOP msg
        # Attributes used by sellers
        self.next_restock_ts = datetime.now() + timedelta(0,random.randint(12,15))  # days, seconds
        self.revenue = 0
        self.prices = {
            "SALT": 1,
            "FISH": 5,
            "BOAR": 10
        }
        # Attributes used by buyers
        self.next_buy_ts = datetime.now() + timedelta(0,random.randint(1,10))  # days, seconds
        # Attributes use by leaders and for elections
        self.resigned = False  # Set to true temporarily when we resign
        self.leader_log = pd.DataFrame(columns=["uid", "timestamp", "clock", "sender", "type", "item", "quantity", "status", "init_time", "acked_time", "received_time"])
        self.elections = dict()  # keys are UIDs, values are timestamps
        self.next_resign_ts = None
        self.last_election_ts = None
        self.leader_log_path = Path(r"leader_log.csv")
        self.leader_clock_path = Path(r"leader_clock")
        self.timestamp_record_path = Path(f"performance test logs/node_{self.id}_timestamps.csv")
        # Optional attributes used for testing
        self.shopping_list = shopping_list
        self.selling_list = selling_list
        self.max_requests = max_requests
        self.cur_requests = 0
        # Locks
        self.is_leader_lock = None  # Used when sending acks to make sure acked transaction gets into log
        self.node_log_lock = None
        self.leader_log_lock = None  # Used when we only want to do something while leader, e.g., acking messages.
        self.clock_lock = None # Used to update the clock when a new request is made
        self.leader_clock_lock = None # Used when the current node is elected as leader.
        self.revenue_lock = None # Used when incrementing revenue on making a sale
        
    
    def start(self):
        """
        Called by parent process to start node running.
        Sets self.running to True, sets up socket, and starts run loop.
        """
        print(f"{datetime.now()}, status, node {self.id} starting using port {self.port_number}")
        # Set up server socket
        self.server_socket = socket.socket()
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.settimeout(100)  # Time out so we can gracefully exit once we stop seeing messages.
        self.server_socket.bind((socket.gethostname(), self.port_number))
        self.server_socket.listen(100000)
        # Set up locks
        self.is_leader_lock = threading.Lock()
        self.node_log_lock = threading.Lock()
        self.leader_log_lock = threading.Lock()
        self.clock_lock = threading.Lock()
        self.leader_clock_lock = threading.Lock()
        self.revenue_lock = threading.Lock()
        # Start run loop (after waiting 5 seconds so all nodes are online)
        time.sleep(10)
        self.running = True
        self.run_loop()
        return
    
    def stop(self):
        """
        Called when we receive stop message from parent process.
        Sets self.running to False, which will end our run loop.
        Closes server socket.
        """
        print(f"{datetime.now()}, status, node {self.id} stopping")
        time.sleep(10)
        self.running = False
        self.server_socket.close()
        # Save all data for debugging
        with self.node_log_lock:
            self.node_log.to_csv(f"logs/node_{self.id}_log.csv")
        with self.leader_log_lock:
            self.leader_log.to_csv(f"logs/node_{self.id}_leader_log.csv")
        with self.leader_clock_lock:
            with open(f"logs/node_{self.id}_leader_clock.txt", "wb") as file:
                pickle.dump(self.leader_clock, file)
        with self.clock_lock:
            with open(f"logs/node_{self.id}_clock.txt", "wb") as file:
                pickle.dump(self.clock, file)
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
                if self.stopped:
                    break
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
                            self.cur_requests += 1
                    if self.is_buyer:
                        if datetime.now() > self.next_buy_ts:
                            self.buy()
                            self.cur_requests += 1
                    if self.max_requests != -1 and self.cur_requests > self.max_requests:
                        self.node_log.to_csv(self.timestamp_record_path)
                        msg = dict(type=ControlMsgType.STOP.name)
                        for nid in self.nodes.keys():
                            self.send_msg(msg=msg, dest=nid)
                        time.sleep(1)
                        self.stop()
                    if True:
                        # Check if any entries have lingered too long (which we'll take to mean
                        # the leader went down), or if any need to be resent
                        self.review_node_log()
                elif self.is_leader:
                    # If we are the leader, check if we've caught up to any pending
                    # transactions in the leader log. If so, handle these.
                    # Also check if it's been long enough that we need to resign.
                    # When we come back after resigning, start a new election.
                    # by choosing a random time delta at time of winning.
                    self.review_leader_log()
                    last_election_ts = self.get_most_recent_election(status=ActionStatus.DONE.name)
                    if last_election_ts is not None:
                        next_resign_ts = last_election_ts + timedelta(0, 60)  # days, seconds
                        #next_resign_ts = last_election_ts + timedelta(0, 10)  # days, seconds
                    else:  # This shouldn't happen, but in we get here before log is saved, we won't resign yet
                        next_resign_ts = datetime.now() + timedelta(0, 10)
                    if datetime.now() > next_resign_ts:
                        stopped = self.resign(sleep=True)
                        if not stopped:
                            self.elect()
                        else:
                            self.stopped = True
        return
    
    def accept_msgs(self, executor) -> bool:
        """
        Called by run_loop to iterate through socket messages and parse each.
        Returned bool indicates whether we received the STOP message.
        If returning True, we did receive a stop message.
        """
        stopped = False
        # We will continue this loop until there are no incoming messages.
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
            # If the msg isn't a STOP, spawn a thread to handle it.
            # But if it is a stop, we handle it here, such that we
            # can call stop and return False (and thus immediately exit the run_loop)
            if msg["type"] != ControlMsgType.STOP.name:
                executor.submit(self.handle_msg, msg, addr)
            else:
                self.stop()
                stopped = True
                break
        return stopped
    
    def handle_msg(self, msg, addr):
        """
        Unpickles msg, checks type, and handles it as needed.
        """
        # Parse message type and call the corresponding function
        match msg["type"]:
            case BuyMsgType.INIT.name:
                # For buy messages, we increment this node's vector clock.
                # If this node is the leader, we also add the node to the leader log and
                # send an ack. Note that the leader log is used to store messages
                # until our clock is such that we can accept them.
                self.clock_lock.acquire()
                self.update_vector_clock_received_message(msg["clock"], msg["sender"])
                self.clock_lock.release()
                self.is_leader_lock.acquire_lock()
                self.leader_log_lock.acquire()
                if self.is_leader:
                    self.append_to_leader_log(msg, transaction_type=ActionType.BUY.name)
                    self.send_ack(uid=msg["uid"], dest=msg["sender"])
                self.leader_log_lock.release()
                self.is_leader_lock.release_lock()
            case BuyMsgType.RESTOCK.name:
                # Similar to buy request. Increment node clock as needed, and
                # update leader log if node is leader.
                self.clock_lock.acquire()
                self.is_leader_lock.acquire_lock()
                self.update_vector_clock_received_message(msg["clock"], msg["sender"])
                self.is_leader_lock.release_lock()
                self.clock_lock.release()
                self.is_leader_lock.acquire_lock()
                if self.is_leader:
                    self.leader_log_lock.acquire()
                    self.append_to_leader_log(msg, transaction_type=ActionType.RESTOCK.name)
                    self.send_ack(uid=msg["uid"], dest=msg["sender"])
                    self.leader_log_lock.release()
                self.is_leader_lock.release_lock()
            case BuyMsgType.PAYMENT.name:
                # When we get payed for selling an item,
                # reduce the item sold's quantity and record revenue.
                self.node_log_lock.acquire()
                self.node_log.loc[self.node_log["uid"] == msg["uid"], "quantity"] -= msg["quantity"]
                self.node_log.loc[self.node_log["uid"] == msg["uid"], "status"] = msg["status"]
                self.revenue_lock.acquire()
                self.revenue += msg["quantity"] * self.prices[msg["item"]]
                print(f"{datetime.now()}, {msg["uid"]}, payment of {msg["quantity"] * self.prices[msg["item"]]} to {self.id} made for selling {msg["quantity"]} of {msg['item']}")
                self.revenue_lock.release()
                self.node_log_lock.release()
            case BuyMsgType.FINISH_TRANSACTION.name:
                # If our submitted buy is being finished here, we
                # mark the transactiona s done and print the results
                # of transaction (amount bough and amount payed)
                self.node_log_lock.acquire()
                self.node_log.loc[self.node_log["uid"] == msg["uid"], "status"] = ActionStatus.DONE.name
                if msg["quantity"] > 0:
                    print(f"{datetime.now()}, {msg["uid"]}, Node {self.id} purchased {msg['quantity']} {msg["item"]}.")
                else:
                    print(f"{datetime.now()}, {msg["uid"]}, Node {self.id} failed to purchase {msg["item"]}, there were none in stock when attempting to purchase.")
                self.node_log.loc[self.node_log["uid"] == msg["uid"], "received_time"] = time.time()
                self.node_log_lock.release()
            case ControlMsgType.ACK.name:
                # If msg is an ack, we'll record it in node log.
                self.recieve_ack(msg)
            case ElecMsgType.ELECT.name:
                # If message is an ELECT, we act according to bully algorithm.
                # If we have a higher id than sender, send back an OK and initiate
                # our own election. Otherwise, don't respond.
                if self.id > msg["sender"]:
                    self.okay(msg)
                    self.elect()
            case ElecMsgType.OKAY.name:
                # If message is an OKAY, we lost the election, and will wait
                # for a new leader to send out an IWON (or until a certain
                # time interval elapses and we start a new election). im_okay()
                # facilitates this reaction.
                self.im_okay(uid=msg["uid"])
            case ElecMsgType.IWON.name:
                # If we receive an IWON, we call youwon, which will set the leader
                # to the msg's sender, and update node attributes as needed to
                # reflect the change in leader.
                #self.node_log.to_csv(f"logs/node_{self.id}_log.csv")  # For debugging, we save node log
                self.youwon(new_leader = msg["sender"])
            case ControlMsgType.STOP.name:
                # If we receive a STOP message, it's time to shut down.
                # Call self.stop() to facilitate this.
                self.stop()
        return
    
    def send_msg(self, msg, dest:int):
        """
        Called anytime we need to send a msg.
        Send message to node whose ID is dest.
        """
        # We used the below to debug messages not making it into the leader_log.
        # It helped us identify when nodes sent messages whose clock differed from what
        # was in their log.
        if "clock" in list(msg.keys()):
            msg_clock_time = int(msg["clock"][self.id])
            prev_clock_times = [d[self.id] for d in self.node_log["clock"].to_list() if self.id in d.keys()]
            try:
                if (msg_clock_time - 1 > 0) and (msg_clock_time - 1 not in prev_clock_times):
                    #raise Exception
                    pass
            except Exception as e:
                print(e)
                raise Exception
        # Increment num msgs sent and send the message.
        # The try statement is left over from when we
        # we debugging msg sending errors.
        self.num_sent_msgs += 1
        port = self.nodes[dest]
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as node_socket:
                node_socket.connect((socket.gethostname(), port))
                serialized_msg = pickle.dumps(msg, -1)  # -1 is used to pick best representation
                node_socket.sendall(serialized_msg)
                node_socket.close()
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            raise Exception
        return

    def resend_msg(self, row: DataFrame):
        """
        Called anytime we need to resend a message.
        The log row containing the message information is used
        to build the message.
        """
        # If this is the beginning of a buy message,
        # we send a buy message and update the log with the new status
        # (started STARTED of NEEDS_RESEND), and update the corresponding
        # timestamp in the log.
        if row["type"] == BuyMsgType.INIT.name:
            msg = dict(
                uid = row["uid"],
                sender = self.id,
                clock = row["clock"],
                type = BuyMsgType.INIT.name,
                item = row["item"],
                quantity = row["quantity"],
            )
            self.node_log.loc[self.node_log["uid"] == row["uid"], "status"] = ActionStatus.STARTED.name
            self.node_log.loc[self.node_log["uid"] == row["uid"], "timestamp"] = datetime.now()
            self.send_msg(msg, self.leader)
            print(f"{datetime.now()}, {msg["uid"]}, {msg["type"]}, resending msg")
        # If this is a restock message,
        # we send a buy message and update the log with the new status
        # (started STARTED of NEEDS_RESEND), and update the corresponding
        # timestamp in the log.
        elif row["type"] == BuyMsgType.RESTOCK.name:
            msg = dict(
                uid = row["uid"],
                sender = self.id,
                clock = row["clock"],
                type = BuyMsgType.RESTOCK.name,
                item = row["item"],
                quantity = row["quantity"],
            )
            self.node_log.loc[self.node_log["uid"] == row["uid"], "status"] = ActionStatus.STARTED.name
            self.node_log.loc[self.node_log["uid"] == row["uid"], "timestamp"] = datetime.now()
            self.send_msg(msg, self.leader)
            print(f"{datetime.now()}, {msg["uid"]}, {msg["type"]}, resending msg")
    
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
        # Note: importantly, this function is only ever called when we
        # have picked up the is_leader_lock. Otherwise, we could
        # get past the if statement checking that we're the leader,
        # then lose the leadership. We don't want non-leaders to
        # send ACKs, as this can mess up the logging.
        if self.is_leader:
            self.send_msg(msg=msg, dest=dest)
            #print(f"{datetime.now()}, {uid}, node {self.id} sent ack")
        return
    
    def recieve_ack(self, msg):
        """
        When leader sends an initial ACK back, the node marks that
        transaction as ACKED in the node log.
        """
        uid = msg["uid"]
        #print(f"{datetime.now()}, {uid}, node {self.id} received ack")
        self.update_node_log(uid=uid, timestamp=datetime.now(),
                             status=ActionStatus.ACKED.name)
        return

    def buy(self):
        """
        Initiate buy request by selecting random item + quantity and messaging leader.
        Alternatively, if a shopping list was assigned to this (which we use
        for testing cases), we get the item and quantity from the shopping list.
        """
        self.clock_lock.acquire() # Acquire lock for whole fct to avoid double increments
        # Either pick random item and quantity or get from shopping list
        uid = uuid.uuid4()
        if len(self.shopping_list) == 0:
            item = random.choice(list(Item)).name
            quantity = random.choice(range(1, 10))
        else:
            item_quantity_dict = self.shopping_list.pop()
            item = list(item_quantity_dict.keys())[0]
            quantity = item_quantity_dict[item]

        # Update vector clock and add to node log
        self.update_vector_clock_local_event()
        clock_lock_copy = copy.deepcopy(self.clock)

        self.append_to_node_log(uid=uid, timestamp=datetime.now(), clock=clock_lock_copy,
                                type=BuyMsgType.INIT.name, item=item, quantity=quantity,
                                status=ActionStatus.STARTED.name)

        print(f"{datetime.now()}, {uid}, node {self.id} is buying {item}")
        # Send out request
        msg = dict(
            uid = uid,
            sender = self.id,
            clock = clock_lock_copy,
            type = BuyMsgType.INIT.name,
            item = item,
            quantity = quantity
        )
        for nid in self.nodes.keys():  # Multicast request so all nodes can update vector clocks
            self.send_msg(msg=msg, dest=nid)
        self.next_buy_ts = datetime.now() + timedelta(0, random.choice(range(1, 10)))
        self.clock_lock.release()
        return
    
    def finalize_buy(self, row):
        """
        When the leader's clock catches up to a BUY request, we call this function
        to actually process that buy. This consists of checking the items available for
        sale, sending a message telling the buyer how much they bough, and sending payment
        to the seller(s).
        """
        if row["sender"] == self.id:
            # the leader does not buy or sell, so we effectively discard
            # the leader's purchases. Note that these were started
            # before the node became leader.
            pass
        else:
            with self.leader_log_lock:  # acquire lock so we sell each item only once
                # Pull out postings we could buy from
                requested_quantity = row["quantity"]
                postings = self.leader_log.copy()
                postings = postings[ (postings["type"] == ActionType.RESTOCK.name)
                                    & (postings["item"] == row["item"])
                                    & (postings["sender"] != self.id) ]
                
                # While there is no way to tell which of two concurrent events
                # come first, the following sort does ensure that if an request's clock
                # is strictly before another request's clock, then it will come before
                # that request in our dataframe, and thus sell first too.
                postings = postings.reset_index(drop=True)
                clock_df = pd.DataFrame(postings["clock"].to_list())
                clock_cols = clock_df.columns.to_list()
                postings[clock_cols] = clock_df
                postings = postings.sort_values(by=clock_cols).reset_index(drop=True)
                # Figure out which postings we'll buy items from
                postings["cumsum"] = postings["quantity"].cumsum()
                postings["left over"] = np.where(postings["cumsum"] - requested_quantity > 0,
                                                postings["cumsum"] - requested_quantity,
                                                0)
                used_postings = postings[postings["left over"] < postings["quantity"]]
                # Figure out how much we'll buy from each posting, and then update the log
                used_postings["amount bought"] = used_postings["quantity"] - used_postings["left over"]
                used_uids = used_postings["uid"].to_list()
                # We wrapped these lines in a try statement when implementing them,
                # such that we could debug the pandas and numpy operations.
                try:
                    self.leader_log["quantity"] = np.where(self.leader_log["uid"].isin(used_uids),
                                                        self.leader_log["uid"].map(used_postings.set_index("uid")["left over"]),
                                                        self.leader_log["quantity"])
                    self.leader_log["status"] = np.where(self.leader_log["uid"].isin(used_uids) & (self.leader_log["quantity"] == 0),
                                                        ActionStatus.DONE.name,
                                                        self.leader_log["status"])
                except Exception as e:
                    print(e)
                    raise Exception
            # Send message to buyer telling them how much we bought.
            bought_quantity = int(used_postings["amount bought"].sum())
            msg = dict(
                uid = row["uid"],
                sender = self.id,
                clock = row["clock"],
                type = BuyMsgType.FINISH_TRANSACTION.name,
                item = row["item"],
                quantity = bought_quantity,
                status = ActionStatus.DONE.name
            )
            self.send_msg(msg, row["sender"])
            # Finally, pay sellers as needed
            for i, post in used_postings.iterrows():
                msg = dict(
                    uid = post["uid"],
                    sender = self.id,
                    clock = post["clock"],
                    type = BuyMsgType.PAYMENT.name,
                    item = post["item"],
                    quantity = post["amount bought"],
                    status = ActionStatus.DONE.name
                )
                self.send_msg(msg, post["sender"])
        return

    def restock(self):
        """
        Seller picks new item, stocks a certain amount of it, and sends message to leader indicating this.
        Alternatively, if a selling list was assigned to this (which we use
        for testing cases), we get the item and quantity from the selling list.
        """
        self.clock_lock.acquire()
        uid = uuid.uuid4()
        # Either pick random item and quantity, or get them from selling list.
        if len(self.selling_list) == 0:
            item = random.choice(list(Item)).name
            quantity = 20
        else:
            item_quantity_dict = self.selling_list.pop()
            item = list(item_quantity_dict.keys())[0]
            quantity = item_quantity_dict[item]
        # Update vector clock and node log
        self.update_vector_clock_local_event()
        clock_lock_copy = copy.deepcopy(self.clock)
        self.append_to_node_log(uid=uid, timestamp=datetime.now(), clock=clock_lock_copy,
                                type=BuyMsgType.RESTOCK.name, item=item, quantity=quantity,
                                status=ActionStatus.STARTED.name)
        print(f"{datetime.now()}, {uid}, node {self.id} is restocking {item}")
        # Send out request
        msg = dict(
            uid=uid,
            sender=self.id,
            clock=clock_lock_copy,
            type=BuyMsgType.RESTOCK.name,
            item=item,
            quantity=quantity
        )
        for nid in self.nodes.keys():
            self.send_msg(msg=msg, dest=nid)
        self.next_restock_ts = datetime.now() + timedelta(0, 10)
        self.clock_lock.release()
        return

    def review_node_log(self):
        """
        Called by each non-leader node during the run_loop.
        If any items in the node log have status NEEDS_RESEND, resend them.
        If transactions have gone unACKED for too long,
        set self.leader to None to trigger election and set their status to NEEDS_RESEND.
        """
        self.node_log_lock.acquire_lock()
        # Check how long the oldest request has gone unACKed.
        # If it has been too long, we assume the leader has gone down
        # and set self.leader to None (which will trigger an election in run_loop)
        try:
            last_ack_ts = self.node_log[self.node_log["status"] == ActionStatus.ACKED.name]["timestamp"].max()
            longest_started_ts = self.node_log[self.node_log["status"] == ActionStatus.STARTED.name]["timestamp"].min()
            if not pd.isna(longest_started_ts):
                if longest_started_ts + timedelta(0, 30) < datetime.now():
                    self.leader = None
                    self.node_log.loc[self.node_log["status"] == ActionStatus.STARTED.name,
                                          "status"] = ActionStatus.NEEDS_RESEND.name
        except Exception as e:
            print(e)
            raise Exception
        # Assuming leader is still up, resend messages marked with NEEDS_RESEND
        if self.leader is not None:
            log = self.node_log.copy()
            log = log[log["status"] != ActionStatus.DONE.name]
            log = log[log["status"] == ActionStatus.NEEDS_RESEND.name]
            for i, row in log.iterrows():
                uid = row["uid"]
                status = row["status"]
                timestamp = row["timestamp"]
                if status == ActionStatus.NEEDS_RESEND.name:
                    self.resend_msg(row)
        self.node_log_lock.release_lock()
        return
    
    def review_leader_log(self):
        """
        Called in each run_loop by the leader.
        Check if we've caught up to the clock of any transactions still in the log.
        If so, we can process those transactions.
        """
        # Make a copy of the leader log so we don't have to lock it the whole time.
        with self.leader_log_lock:
            log = copy.deepcopy(self.leader_log)
        # Iterate through entries in log that aren't DONE yet. If our clock has caught up to a given entry,
        # we'll process it now.
        log = log[(log["status"] != ActionStatus.DONE.name) & (log["status"] != ActionStatus.NEEDS_RESEND.name)]
        self.leader_clock_lock.acquire_lock()
        for i, row in log.iterrows():
            # hack-y fix to get around datatype problems when reading from CSV, should probably be done differently
            # This may no longer be needed.
            if type(row["clock"]) is str:
                row["clock"] = {int(key): int(val) for key, val in [item.split(': ') for item in row["clock"][1:-1].split(', ')]}

            valid_clock_diff = self.verify_leader_clock_valid(row["clock"], row["sender"])
            if valid_clock_diff is True:
                # process request if clock is ready.
                if (log.loc[log["uid"] == row["uid"], "status"] == ActionProcessStatus.RECIEVED.name).any():
                    if (log.loc[log["uid"] == row["uid"], "type"]  == ActionType.BUY.name).any():
                        self.finalize_buy(row)
                        self.update_leader_log(uid=row["uid"], timestamp=datetime.now(), status=ActionStatus.DONE.name)

                # update leader clock
                self.update_vector_clock_received_message_leader(row["clock"], row["sender"])
        self.leader_clock_lock.release_lock()

        return

    def verify_leader_clock_valid(self, clock: dict, sender: int):
        """
        Used to check if the current processing event is allowed next by the vector clock orderings.
        We implement some vector clock logic here.
        """
        return_bool = True
        for key in clock.keys():
            node_clock_val = self.leader_clock[key]
            other_node_clock = clock[key]
            if key == sender:  # Check that this is the next exected msg from sender
                return_bool = return_bool and other_node_clock == node_clock_val + 1
            else:  # Check that we've seen every message from other nodes that sender has
                return_bool = return_bool and other_node_clock <= node_clock_val
        return return_bool

    def resign(self, sleep:bool):
        """
        Called when leader resigns.
        Saves leader log.
        Sets self.is_leader to False.
        If sleep is True, we also wait for a while here to simulate a node
        being down (a leader calling in sick). While down, we keep
        the queue empty.
        """
        # Pick up log lock, and save log. Make sure to
        # set is_leader to False before releasing lock,
        # so that one we stop updating the log,
        # our message handling won't send anymore ACKs,
        # and the node won't otherwise act as leader
        print(f"{datetime.now()}, election, node {self.id} is resigning")
        self.is_leader_lock.acquire_lock()
        self.leader_clock_lock.acquire_lock()
        self.leader_log_lock.acquire_lock()
        self.is_leader = False
        self.leader = None
        stopped = False
        # Save leader log and leader clock to disk
        self.leader_log[self.leader_log["status"] != ActionStatus.DONE.name].to_csv(self.leader_log_path, index=False)
        with open(self.leader_clock_path, "wb") as file:
            pickle.dump(self.leader_clock, file)
            #print(f"{str(self.leader_clock)}")
        self.leader_log = self.leader_log.drop(self.leader_log.index)  # wipe out leader log from this node
        self.leader_log_lock.release_lock()
        self.leader_clock_lock.release_lock()
        self.is_leader_lock.release_lock()
        # Go offline for wait_interval seconds. Keep socket queue clear while offline.
        if sleep:
            wait_interval = random.choice(range(100, 110))
            wait_time = 0
            while wait_time < wait_interval:
                while select.select([self.server_socket], [], [], 0.1)[0]:
                    socket_connection, addr = self.server_socket.accept()
                    data = socket_connection.recv(4096)
                    msg = pickle.loads(data)
                    # Most msgs we recieve while resigned we just drop.
                    # We do process STOP messages to allow for graceful
                    # exits once the main process reaches the end of
                    # its runtime.
                    if msg["type"] == ControlMsgType.STOP.name:
                        self.stop()
                        stopped = True
                        wait_time = wait_interval + 1
                        break
                time.sleep(1)
                wait_time += 1
            if stopped:
                return stopped
            print(f"{datetime.now()}, status, node {self.id} is back online")
        return stopped
    
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
        # We'll only pick up the log from the disk if we weren't already the leader,
        # so we record our prior leader status here.
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
        self.node_log_lock.release_lock()
        # Wait a few seconds, then pick up the log. This gives old leader time to save it.
        time.sleep(15)
        if not already_leader:
            self.leader_clock_lock.acquire_lock()
            self.leader_log_lock.acquire_lock()
            if self.leader_log_path.exists():
                # Perhaps foolishly, we store clocks as dicts in the dataframe column.
                # When we read that from disk, pandas reads it in as a string. We here
                # parse the string to make it a dict again.
                disk_log = pd.read_csv(self.leader_log_path)
                clock_df = pd.DataFrame(disk_log["clock"].str.strip("{}").str.split(", ").to_list())
                for c in clock_df.columns: clock_df[c] = clock_df[c].str.slice(3)
                clock_df = clock_df.astype(int)
                disk_log["clock"] = pd.Series(clock_df.T.to_dict())
                self.leader_log = pd.concat([disk_log, self.leader_log]).drop_duplicates(subset=["uid"], keep="last")
            if self.leader_clock_path.exists():
                # Read clock from disk.
                with open(self.leader_clock_path, "rb") as file:
                    disk_clock = pickle.load(file)
                    disk_clock_is_old = True
                    # We only read the clock from disk if it's ahead of our current leader clock.
                    for i in disk_clock.keys(): disk_clock_is_old = disk_clock_is_old and disk_clock[i] <= self.leader_clock[i]
                    if not disk_clock_is_old:
                        self.leader_clock = disk_clock
                        #print(f"{str(self.leader_clock)}")
            # Any transactions in this node's log that haven't been
            # acked yet need to be added to leader log, otherwise
            # this leader's clock will stagnate, and this leader
            # won't be able to process
            self.node_log_lock.acquire()
            try:
                self.node_log_to_leader_log()
            except Exception as e:
                print(e)
                raise Exception
            self.node_log_lock.release()
            self.leader_log_lock.release_lock()
            self.leader_clock_lock.release_lock()
        return
    
    def youwon(self, new_leader):
        """
        Called when we receive an IWON message.
        If this node is the leader, resign (which will save the leader log).
        Marks all ongoing elections as done.
        """
        # If we're the current leader, resign.
        if self.is_leader:
            self.resign(sleep=False)
        self.leader = new_leader
        # Mark all ongoing elections as done
        self.node_log_lock.acquire_lock()
        election_mask = (self.node_log["type"] == ActionType.ELECT.name)
        started_mask = (self.node_log["status"] == ActionStatus.STARTED.name)
        self.node_log.loc[election_mask & started_mask, "status"] = ActionStatus.DONE.name
        self.node_log.loc[election_mask & started_mask, "timestamp"] = datetime.now()
        # And we'll set all unACKed items as needing a resend
        self.node_log.loc[self.node_log["status"] == ActionStatus.STARTED.name,
                          "status"] = ActionStatus.NEEDS_RESEND.name
        self.node_log_lock.release_lock()
        return

    def get_most_recent_election(self, status:str):
        """
        Get most recent election timestamp. Status tells us
        if we're looking for DONE or STARTED elections.
        Used to figure out when we've won an election (and thus need to send an IWON)
        and when the last finished election was (so if it was a long time ago, and there's
        still no leader, we can start a new election).
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
        self.node_log_lock.acquire_lock()
        log_entry = dict(
            uid = uid,
            timestamp = timestamp,
            clock = clock,
            type = type,
            item = item,
            quantity = quantity,
            status = status,
            init_time = time.time()
        )
        try:
            if uid not in self.node_log["uid"].to_list():
                # Only add the entry if it's not already in the log.
                self.node_log.loc[len(self.node_log)] = log_entry
        except Exception as e:
            print(e)
            raise Exception
        self.node_log_lock.release_lock()
        return


    def update_node_log(self, uid, timestamp, status: str):
        """
        Update this UID's timestamp and status in node log.
        """
        self.node_log_lock.acquire_lock()
        curr_status = self.node_log[self.node_log["uid"] == uid]["status"].iloc[0]
        if curr_status != ActionStatus.DONE.name:
            # Only update UID's status if the UID isn't DONE yet.
            self.node_log.loc[self.node_log["uid"] == uid, "status"] = status
            self.node_log.loc[self.node_log["uid"] == uid, "timestamp"] = timestamp
        self.node_log_lock.release_lock()
        if status == ActionStatus.ACKED.name:
            self.node_log.loc[self.node_log["uid"] == uid, "acked_time"] = time.time()
        return
    
    def node_log_to_leader_log(self):
        """
        This function adds any NEEDS_RESEND messages from the node log to the leader log.
        This is so that when a node becomes leader, it stops sending BUY and RESTOCK
        requests. Any NEEDS_RESEND messages have been sent out but were never added to
        the leader log. That means other nodes have clocks reflect those messages, but the leader
        log doesn't ahve those messages. These facts combine to mean that, if we don't add
        these messages to the leader log, then the leader clock will stagnate until this node
        is no longer the leader. Hence, we do add NEEDS_RESEND msgs to the log here.
        """
        # Get the unacked transcations from node log and update them to match leader log formatting
        node_log = self.node_log.copy()
        unacked_transactions = node_log[node_log["status"].isin([ActionStatus.STARTED.name, ActionStatus.NEEDS_RESEND.name]) ]
        unacked_transactions = unacked_transactions[unacked_transactions["type"].isin([BuyMsgType.INIT.name, BuyMsgType.RESTOCK.name])]
        unacked_transactions.loc[unacked_transactions["type"] == BuyMsgType.INIT.name, "type"] = ActionType.BUY.name
        unacked_transactions["sender"] = self.id
        unacked_transactions["status"] = ActionProcessStatus.RECIEVED.name
        # Add these transactions to leader log, and then set their status in the node log as ACKED.
        self.leader_log = pd.concat([self.leader_log, unacked_transactions]).reset_index(drop=True)
        self.node_log["status"] = np.where(self.node_log["uid"].isin(unacked_transactions["uid"].to_list()),
                                           ActionStatus.ACKED.name,
                                           self.node_log["status"])
        # Below statements used for debugging.
        if self.id == 3:
            pass
        if (self.leader_log.groupby("uid").count() > 1).any().any():
            pass
        return
    
    def append_to_leader_log(self, msg, transaction_type):
        """
        Append transaction from msg to leader log. If we already have this UID in the log,
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
        if msg["uid"] not in self.leader_log["uid"].to_list():
            self.leader_log.loc[len(self.leader_log)] = log_entry
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
        for i in self.clock.keys():
            self.clock[i] = max(received_clock[i], self.clock[i])

    def update_vector_clock_received_message_leader(self, received_clock: dict, sender_id: int):
        """
        Performs the update to the local leader clock when appropriate.
        """
        for i in self.leader_clock.keys():
            self.leader_clock[i] = max(received_clock[i], self.leader_clock[i])






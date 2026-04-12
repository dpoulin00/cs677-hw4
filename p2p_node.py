import pickle
import random
import socket
import select
import time
import threading
import uuid
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import timedelta
from datetime import datetime
from enum import Enum
import pandas as pd


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
    INIT = 0  # Buyer sends to leader to start buy process (multicast, increment clock)
    RESPONSE = 1  # Leader responds to buyer (unicast)
    PAYMENT = 2  # Leader pays seller (unicast)
    RESTOCK = 3  # Seller sends to leader to stock new inventory (unicast, increment clock)

class ElecMsgType(Enum):
    RESIGN = 0  # Leader multicasts to all nodes to resign (multicast, increment clock)
    ELECT = 1  # Node tries to elect self. (multicast to upstream nodes)
    OKAY = 2  # Response to ELECT when we have higher ID. (unicast)
    IWON = 3  # If nobody responds to ELECT, we're the new leader. (multicast)

class ControlMsgType(Enum):
    STOP = 0  # Parent process sends to shuts down the node
    # Below msgs could be used to include stop conditions for network
    REPORT_CMD = 1  # Parent process requests status from nodes
    REPORT = 2  # Node sends status to parent process
    REPLY = 3  # Leader sends this back to each node after a msg. A lack of this tells node leader resigned.

class ActionType(Enum):
    ELECT = 0
    BUY = 1
    RESTOCK = 2

class ActionStatus(Enum):
    STARTED = 0  # Action started, but not acked by leader
    ACKED = 1  # Action acked by leader
    DONE = 2  # Action finished.


class P2PNode:
    def __init__(self, id: int, port_number: int, is_buyer: bool, is_seller: bool,
                 nodes: dict[int, int],  # keys are IDs, vals are ports
                 shopping_list: list[dict] | None=None,
                 selling_list: list[dict] | None=None):
        """
        Initializes node by recording whether eacah is a buyer or a seller,
        and a list of neighbors (all nodes in network). Also sets up
        some data structures we'll need.
        """
        # Unique to each node
        self.id = id
        self.port_number = port_number
        self.server_socket = socket.socket()
        self.node_log = pd.DataFrame(columns=["uid", "type", "status", "timestamp"])
        # Attributes used by all nodes
        self.nodes = nodes
        self.clock = {id:0 for id in nodes.keys()}
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
        self.sales = pd.DataFrame(columns=["timestamp", "id", "buyer", "item", "seller", "state"])
        self.inventory = pd.DataFrame(columns=["timestamp", "item", "seller", "count", "price"])
        self.elections = dict()  # keys are UIDs, values are timestamps
        self.next_resign_ts = None
        self.last_election_ts = None
        # Optional attributes used for testing
        self.shopping_list = shopping_list
        self.selling_list = selling_list
        
    
    def start(self):
        """
        Sets self.running to True, sets up socket, and starts run loop
        Since we only have one loop, no need to spawn a thread for run loop.
        """
        # Set up server socket
        self.server_socket = socket.socket()
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.settimeout(100)  # Time out so we can gracefully exit once we stop seeing messages.
        self.server_socket.bind((socket.gethostname(), self.port_number))
        self.server_socket.listen(100)
        # Start run loop
        self.running = True
        print(f"{datetime.now()}, status, node {self.id} starting using port {self.port_number}")
        self.run_loop()
        return
    
    def stop(self):
        """
        Called when receiving stop message. Sets self.running to False, which
        will end our run loop.
        """
        print(f"{datetime.now()}, status, node {self.id} stopping")
        self.running = False
        self.server_socket.close()
        return
    
    def accept_msgs(self, executor) -> bool:
        """
        Call this to iterate through socket messages and parse each.
        Returned bool indicates whether we received the STOP message.
        If returning True, we did receive it.
        """
        stopped = False
        # Check if there's a new message. If so, handle it.
        while select.select([self.server_socket], [], [], 0.1)[0]:
            socket_connection, addr = self.server_socket.accept()
            data = socket_connection.recv(4096)
            socket_connection.close()
            try:
                msg = pickle.loads(data)
            except Exception as e:
                # Helpful for debugging multiple threads, processes
                print(f"Pickle exception:")
                print(e)
            if msg["type"] != ControlMsgType.STOP.name:
                msg_thread = executor.submit(self.handle_msg, msg, addr)
            else:
                self.stop()
                stopped = True
                break
        return stopped

    def get_most_recent_election(self, status:str):
        df = self.node_log.copy()
        elect_mask = (df["type"]==ActionType.ELECT.name)
        status_mask = (df["status"]==status)
        past_elections = df[ elect_mask & status_mask]
        if len(past_elections) == 0:
            last_election = None
        else:
            last_election = past_elections["timestamp"].max()
        return last_election
    
    def run_loop(self):
        """
        Called when node starts running. 
        Runs a loop that does the following:
        1. Check for and handle all incoming messages. Spawn thread to handle each.
        If not the leader, it also does the following:
        2. Check if it's time to send a buy request: if so, spawn thread to handle it.
        3. Check if it's time to stock an item; if so, spawn thread to handle it.
        If the leader, it also does the following:
        4. Routinely save logs. (could alternatively do this when resigning)
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
                    # If there's no leader (either bc we just initialized or bc a msg went unacked),
                    # we're in the election logic. If we haven't started an election in a while,
                    # start a new one. Otherwise, wait to get an IWON
                    last_election_ts = self.get_most_recent_election(status=ActionStatus.DONE.name)
                    if (last_election_ts is None) or (last_election_ts + timedelta(0, 60) > datetime.now()):
                        self.elect()
                    else:
                        time.sleep(1)
                elif not self.is_leader:
                    # We have a leader but we're not it. Buy and sell items.
                    # We only buy and sell after certian intervals.
                    if self.is_seller:
                        if datetime.now() > self.next_restock_ts:
                            self.restock()
                    if self.is_buyer:
                        if datetime.now() > self.next_buy_ts:
                            self.buy()
                elif self.is_leader:
                    # If we are the leader, we check if it's been long enough that we need to resign.
                    # When we come back after resigning, start a new election.
                    # FIXME: make this random. Will need to create a self.next_resign_ts to do so.
                    last_election_ts = self.get_most_recent_election(status=ActionStatus.DONE.name)
                    next_resign_ts = last_election_ts + timedelta(0, 5)  # days, seconds
                    if datetime.now() > next_resign_ts:
                        self.resign()
                        self.elect()
        return
    
    def handle_msg(self, msg, addr):
        """
        Unpickles msg, checks type, and calls corresponding function to handle it.
        """
        # Parse message type and call the corresponding function
        match msg["type"]:
            case BuyMsgType.INIT.name:
                pass
            case BuyMsgType.RESPONSE.name:
                pass
            case BuyMsgType.PAYMENT.name:
                pass
            case BuyMsgType.RESPONSE.name:
                pass
            case ElecMsgType.RESIGN.name:
                if self.leader == msg["sender"]:
                    self.leader = None
                self.elect()
            case ElecMsgType.ELECT.name:
                if self.id > msg["sender"]:
                    self.okay(msg)
            case ElecMsgType.OKAY.name:
                self.im_okay(uid=msg["uid"])
            case ElecMsgType.IWON.name:
                self.leader = msg["sender"]
            case ControlMsgType.STOP.name:
                self.stop()
        return
    
    def send_msg(self, msg, dest:int):
        """
        Send message to all nodes in dest.
        dest is a list of node IDs.
        If we are unable to reach a node, we should catch that.
        If the unreachable node is a leader, initiate an election. Otherwise, jsut move on.
        """
        port = self.nodes[dest]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as node_socket:
            node_socket.connect((socket.gethostname(), port))
            serialized_msg = pickle.dumps(msg, -1)  # -1 is used to pick best representation
            node_socket.sendall(serialized_msg)
        return

    def buy(self):
        """
        Initiate buy request by selecting random item and messaging leader.
        """
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

    def resign(self):
        """
        Called when leader resigns.
        Sets self.is_leader to False, and waits a random (long) interval.
        Calls self.elect() after coming back online.
        Blocking here is okay, since this function does nothing else.
        """
        print(f"{datetime.now()}, election, node {self.id} is resigning")
        self.is_leader = False
        time.sleep(random.choice(range(60, 120)))
        # After coming back online, clear queue
        while select.select([self.server_socket], [], [], 0.1)[0]:
            socket_connection, addr = self.server_socket.accept()
            socket_connection.recv(4096)
        return
    
    def elect(self):
        """
        Called when leader goes down, or this node comes back online after resigning.
        Sends elect message to all nodes with higher ID than this node.
        If any response, we wait for new leader.
        If no response, send iwon message.
        """
        # Add election to node log
        uid = uuid.uuid4()
        log_entry = dict(
            type = ActionType.ELECT.name,
            uid = uid,
            timestamp = datetime.now(),
            status = ActionStatus.STARTED.name
        )
        self.node_log.loc[-1] = log_entry
        # If we have the max ID, we win by default.
        if self.id > max(self.nodes.keys()):
            self.iwon(uid)
        # Otherwise, send out ELECT msgs to upstream nodes
        else:
            msg = dict(
                type = ElecMsgType.ELECT.name,
                sender = self.id,
                uid = uid
            )
            for nid in self.nodes.keys():
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
        """Mark corresponding election uid as done."""
        # Record election as finished
        self.node_log.loc[self.node_log["uid"] == uid, "status"] = ActionStatus.DONE.name
        self.node_log.loc[self.node_log["uid"] == uid, "timestamp"] = datetime.now()
        return
    
    def iwon(self, uid):
        """
        Send out an iwon message if no node responds to our elect message.
        """
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
        # Record election as done in node log
        self.node_log.loc[self.node_log["uid"] == uid, "status"] = ActionStatus.DONE.name
        self.node_log.loc[self.node_log["uid"] == uid, "timestamp"] = datetime.now()
        return

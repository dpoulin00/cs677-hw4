import time
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



class P2PNode:
    def __init__(self, id: int, port_number: int, is_buyer: bool, is_seller: bool,
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
        # Attributes used by all nodes
        self.nodes = dict()
        self.clock = dict()
        self.is_buyer = is_buyer
        self.is_seller = is_seller
        self.is_leader = False
        self.running = False  # Set to true and false by parent process
        # Attributes used by sellers
        self.revenue = 0
        self.prices = {
            "SALT": 1,
            "FISH": 5,
            "BOAR": 10
        }
        # Attributes use by leaders
        self.resigned = False  # Set to true temporarily when we resign
        self.sales = pd.DataFrame(columns=["timestamp", "id", "buyer", "item", "seller", "state"])
        self.inventory = pd.DataFrame(columns=["timestamp", "item", "seller", "count", "price"])
        # Optional attributes used for testing
        self.shopping_list = shopping_list
        self.selling_list = selling_list
    
    def set_nodes(self, nodes:dict):
        self.nodes = nodes
        self.clock = {n: 0 for n in nodes.keys()}
    
    def run(self):
        """
        Sets self.running to True and spawns a thread for our run loop.
        """
        return
    
    def stop(self):
        """
        Called when receiving stop message. Sets self.running to False, which
        will end our run loop.
        """
        return
    
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
        while self.running:
            time.sleep(1)  # So we don't spam check if resign period has ended
            while self.running and not self.resigned:
                # Check if there's a new message. If so, handle it.
                #if there's a message:
                #    handle it

                if not self.is_leader:
                    if self.is_seller:
                        # Check if it's time to buy and if so, do so.
                        pass
                    if self.is_buyer:
                        # Check if it's time to buy and if so, do so.
                        pass
        return
    
    def handle_msg(self):
        """
        Unpickles msg, checks type, and calls corresponding function to handle it.
        """
        return
    
    def send_msg(self, dest:list[int]):
        """
        Send message to all nodes in dest.
        dest is a list of node IDs.
        If we are unable to reach a node, we should catch that.
        If the unreachable node is a leader, initiate an election. Otherwise, jsut move on.
        """
        return

    def buy(self):
        """
        Initiate buy request by selection random item and messaging leader.
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

    def lead_resign(self):
        """
        Called when leader resigns.
        Sets self.resign to True, waits a random interval, and then sets self.resign to False.
        Calls self.elect() after coming back online.
        Blocking here is okay, since this function does nothing else.
        """
        return
    
    def lead_elect(self):
        """
        Called when leader goes down, or this node comes back online after resigning.
        Sends elect message to all nodes with higher ID than this node.
        If any response, we wait for new leader.
        If no response, send iwon message.
        """
        return
    
    def lead_okay(self):
        """
        Answers an elect message if this node has higher ID than elector.
        """
        return
    
    def lead_iwon(self):
        """
        Send out an iwon message if no node responds to our elect message.
        """
        return

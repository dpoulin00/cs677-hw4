from enum import Enum


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


class TxMsg:
    def __init__(self, uid, sender, type, item, quantity):
        self.uid = uid
        self.sender = sender
        self.type = type
        self.item = item
        self.quantity = quantity
    
    def to_dict(self) -> dict:
        d = dict(
            uid = self.uid,
            sender = self.sender,
            type = self.type,
            item = self.item,
            quantity = self.quantity,
        )
        return d

class CacheMsg:
    def __init__(self, uid, sender, type, salt_delta, boar_delta, fish_delta):
        self.sender = sender
        self.uid = uid
        self.type = type
        self.salt_delta = salt_delta
        self.boar_delta = boar_delta
        self.fish_delta = fish_delta
    
    def to_dict(self) -> dict:
        raise Exception("not implemented yet")

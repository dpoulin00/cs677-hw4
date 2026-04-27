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
import enums

# Make sure threads fail loudly
def custom_hook(args):
    print(f"Thread failed: {args.exc_type.__name__}: {args.exc_value}")
threading.excepthook = custom_hook

class Warehouse:
    def __init__(self, id:int, port:int, nodes:dict[int, int]):
        # Set up node properties
        self.id = id
        self.port = port
        self.running = False
        # Set up objects for communication
        self.nodes = nodes
        self.server_socket = socket.socket()
        self.handled_uids = []
        # Quantities
        self.locks = dict()
        self.inv = dict(
            SALT = 0,
            BOAR = 0,
            FISH = 0,
        )
        return
    
    def start(self):
        # Start port
        self.server_socket = socket.socket()
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.settimeout(100)  # Time out so we can gracefully exit once we stop seeing messages.
        self.server_socket.bind((socket.gethostname(), self.port))
        self.server_socket.listen(100000)
        # set up locks
        self.locks = dict(
            SALT = threading.Lock(),
            BOAR = threading.Lock(),
            FISH = threading.Lock(),
        )
        # Start running
        self.running = True
        print(f"{datetime.now()}, warehouse, node {self.id} start on port {self.port}")
        self.run_loop()
        return
    
    def run_loop(self):
        """
        Top of call stack. Called from main.py to create subprocess.
        When we're done, set self.running to False so this function returns.
        """
        while self.running:
            self.listen()

    def listen(self):
        """
        Listen for msgs and spawn threads to deal with each.
        """
        # Open thread executor, and enter listening loop
        with ThreadPoolExecutor(max_workers=100) as executor:
            # We will continue this loop until there are no incoming messages.
            while self.running and select.select([self.server_socket], [], [], 0.1)[0]:
                socket_connection, addr = self.server_socket.accept()
                data = socket_connection.recv(4096)
                socket_connection.close()
                try:
                    msg = pickle.loads(data)
                except Exception as e:
                    # Helpful for debugging multiple threads, processes
                    print(f"Pickle exception:")
                    print(e)
                else:
                    executor.submit(self.handle_msg, msg)
            return
    
    def handle_msg(self, msg):
        """
        Called to handle msgs.
        Parses msg type and calls corresponding function.
        """
        match msg["type"]:
            case enums.MsgType.BUY.name:
                self.handle_buy(msg)
            case enums.MsgType.RESTOCK.name:
                self.handle_restock(msg)
            case enums.ControlMsgType.STOP.name:
                self.stop()
            case _:
                print("Invalid msg type sent to warehouse.")
                raise Exception
        return
    
    def stop(self):
        """
        On recieving a STOP msg, call this fct to shut down the warehouse.
        """
        print(f"{datetime.now()}, warehouse, node {self.id} stopping")
        self.server_socket.close()
        self.running = False
        return

    def handle_buy(self, msg:dict):
        """
        On recieving a BUY, call this msg to handle it.
        Checks BUY against how much inventory we have.
        Sells as much as possible, and sends a reply msg back.
        """
        item = msg["item"]
        ordered = msg["quantity"]
        in_store = self.inv[item]
        # Lock attribute, update, and release.
        with self.locks[item]:
            sold = (ordered if in_store > ordered else in_store)
            self.inv[item] -= sold
        self.handled_uids.append(msg["uid"])  # append is thread safe
        # Return msg
        reply = enums.TxMsg(uid=msg["uid"],
                            sender=self.id,
                            type=enums.MsgType.BUY_REPLY.name,
                            item=item,
                            quantity=sold).to_dict()
        self.send_msg(reply, dest=msg["sender"])
        return
    
    def handle_restock(self, msg:dict):
        """
        Called on receiving a RESTOCK msg.
        Adds items to inventory.
        """
        item = msg["item"]
        stocked = msg["quantity"]
        # Lock attribute, update, and release.
        with self.locks[item]:
            self.inv[item] += stocked
        self.handled_uids.append(msg["uid"])  # append is thread safe
        # Return msg
        reply = enums.TxMsg(uid=msg["uid"],
                            sender=self.id,
                            type=enums.MsgType.RESTOCK_REPLY.name,
                            item=item,
                            quantity=stocked).to_dict()
        self.send_msg(reply, dest=msg["sender"])
        return
    
    def send_msg(self, msg:dict, dest:int):
        """
        Sends outgoing msgs to dest.
        We also need to handle fault tolerance in case a trader goes
        down.
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as node_socket:
                dest_port = self.nodes[dest]
                node_socket.connect((socket.gethostname(), dest_port))
                serialized_msg = pickle.dumps(msg, -1)  # -1 is used to pick best representation
                node_socket.sendall(serialized_msg)
                node_socket.close()
        except Exception as e:
            print(f"Haven't implement fault tolerance for warehouse")
            raise Exception
        return




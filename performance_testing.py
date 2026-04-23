import math
import pickle
import random
import socket
import time
from multiprocessing import Process

import numpy as np
import pandas as pd

import p2p_node as p2p
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from pathlib import Path
from main import run_network
from main import make_random_network
import matplotlib.pyplot as plt


def make_performance_test_network(num_nodes: int, start_port, max_requests=-1, node_to_set=None) -> dict[int, p2p.P2PNode]:
    """
    Creates a random network of nodes. Returns dict, whose keys are ports
    and whose values are the nodes. Note that here we specify a few extra fields to allow us to accurately track requests from the various nodes
    """
    network = dict()
    node_ports = {i:start_port+i for i in range(0, num_nodes)}
    # Iterate through once to initialize all nodes.
    for id in range(num_nodes):
        curr_port_number = node_ports[id]
        role = random.choice(list(p2p.Role)).name
        node_view_of_network = node_ports.copy()
        del node_view_of_network[id]
        # Three possible roles, BUYER, SELLER, and BUYER_AND_SELLER
        is_buyer = (True if role in [p2p.Role.BUYER.name, p2p.Role.BUYER_AND_SELLER.name] or node_to_set is not None and id==node_to_set else False)
        is_seller = (True if role in [p2p.Role.SELLER.name, p2p.Role.BUYER_AND_SELLER.name] and (node_to_set is None or id !=node_to_set) else False)
        max_requests = max_requests if node_to_set is not None and id == node_to_set else -1
        network[curr_port_number] = p2p.P2PNode(id=id,
                                                port_number=curr_port_number,
                                                is_buyer=is_buyer,
                                                is_seller=is_seller,
                                                nodes=node_view_of_network,
                                                max_requests=max_requests
                                                )
    return network

if __name__ == "__main__":
    start_port = 49152
    timestamp_path = Path("performance test logs/node_0_timestamps.csv")
    if not Path.exists(timestamp_path):
        network = make_performance_test_network(num_nodes=6, start_port=start_port, max_requests=1000, node_to_set=0)
        run_network(network=network, run_time=10000, stop_network=True)


    request_timestamps = pd.read_csv(timestamp_path)
    # also plotting for a separate run of a node consistently elected as leader
    timestamp_node_4_path = Path("performance test logs/node_4_timestamps.csv")
    request_timestamps_4 = pd.read_csv(timestamp_node_4_path)

    def plot(timestamps, label):
        x_data = []
        y_data = []
        x_data2 = []
        y_data2 = []

        for i, row in timestamps.iterrows():
            if not math.isnan(row["acked_time"]):
                x_data2.append(row.acked_time - row.init_time)
                y_data2.append(random.random() * 20)
            if not math.isnan(row["received_time"]):
                x_data.append(row.received_time - row.init_time)
                y_data.append(random.random() * 20)

        print(f"Average node {label} response time to ack: {np.average(x_data2)}")
        print(f"Average node {label} response time to response: {np.average(x_data)}")

        plt.scatter(x_data, y_data)
        frame = plt.gca()
        frame.axes.get_yaxis().set_ticks([])
        plt.xlabel(f"{label} received time difference")
        plt.show()
        frame = plt.gca()
        frame.axes.get_yaxis().set_ticks([])
        plt.xlabel(f"{label} acked time difference")
        plt.scatter(x_data2, y_data2)
        plt.show()

    plot(request_timestamps, "node 0")
    plot(request_timestamps_4, "node 4")
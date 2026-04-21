import pickle
import random
import socket
import time
from multiprocessing import Process

import pandas as pd

import p2p_node as p2p
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from pathlib import Path
from main import run_network
from main import make_random_network
import matplotlib.pyplot as plt


def make_performance_test_network(num_nodes: int, start_port:int, max_requests:int, nodes_to_limit:list) -> dict[int, p2p.P2PNode]:
    """
    Creates a random networ of nodes. Returns dict, whose keys are ports
    and whose values are the nodse.
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
        is_buyer = (True if role in [p2p.Role.BUYER.name, p2p.Role.BUYER_AND_SELLER.name] and id != 1 else False)
        is_seller = (True if role in [p2p.Role.SELLER.name, p2p.Role.BUYER_AND_SELLER.name] or id == 1 else False)
        max_requests = max_requests if id in nodes_to_limit else -1
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
    log_path = Path("leader_log.csv")
    clock_path = Path("leader_clock")
    timestamp_path = Path("node_0_request_timestamps.csv")
    if log_path.exists():
        log_path.unlink()
    if clock_path.exists():
        clock_path.unlink()
    if timestamp_path.exists():
        timestamp_path.unlink()
    time.sleep(1)  # time for logs to be deleted
    network = make_performance_test_network(num_nodes=6, start_port=start_port, max_requests=1000, nodes_to_limit=[1])
    test = 2
    run_network(network=network, run_time=100, stop_network=True)

    request_timestamps = pd.read_csv(timestamp_path)

    x_data = []
    y_data = []

    for i, row in request_timestamps.iterrows():
        x_data.append(row.time_to_first_response)
        y_data.append(random.random() * 20)
    plt.scatter(x_data, y_data)
    plt.show()


    pass




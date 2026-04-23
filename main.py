import pickle
import random
import socket
import sys
import time
from multiprocessing import Process

import p2p_node as p2p
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from pathlib import Path


def make_random_network(num_nodes: int, start_port) -> dict[int, p2p.P2PNode]:
    """
    Creates a random network of nodes, with random roles.
    Returns dict, whose keys are ports and whose values are the nodes.
    """
    # Start by defining the port for each node.
    network = dict()
    node_ports = {i:start_port+i for i in range(0, num_nodes)}
    # Iterate through to initialize all nodes.
    for id in range(num_nodes):
        curr_port_number = node_ports[id]
        role = random.choice(list(p2p.Role)).name
        # Give this node a list of all node ports except its own.
        node_view_of_network = node_ports.copy()
        del node_view_of_network[id]
        # Three possible roles, BUYER, SELLER, and BUYER_AND_SELLER. These are passed to
        # the node via the following two bools.
        is_buyer = (True if role in [p2p.Role.BUYER.name, p2p.Role.BUYER_AND_SELLER.name] else False)
        is_seller = (True if role in [p2p.Role.SELLER.name, p2p.Role.BUYER_AND_SELLER.name] else False)
        network[curr_port_number] = p2p.P2PNode(id=id,
                                                port_number=curr_port_number,
                                                is_buyer=is_buyer,
                                                is_seller=is_seller,
                                                nodes=node_view_of_network,
                                                )
    return network


def dict_to_network(node_dict: dict) -> dict[int, p2p.P2PNode]:
    """
    For testing, we'd like to be able to specify a network
    whose nodes have predefined nodes and predefined items.
    To that end, this function converts a dictionary, whose keys
    are node IDs and whose values are dictionaries with the keys
    'port', 'role', 'shopping_list', and 'selling_list'.
    """
    network = dict()
    node_ports = {i:node_dict[i]["port"] for i in node_dict.keys()}
    # Iterate through to initialize all nodes.
    for id in node_dict.keys():
        # Pull port and role from passed node dictionary.
        curr_port_number = node_dict[id]["port"]
        role = node_dict[id]["role"]
        # Give this node a list of all node ports except its own.
        node_view_of_network = node_ports.copy()
        del node_view_of_network[id]
        # Three possible roles, BUYER, SELLER, and BUYER_AND_SELLER. These are passed to
        # the node via the following two bools.
        is_buyer = (True if role in [p2p.Role.BUYER.name, p2p.Role.BUYER_AND_SELLER.name] else False)
        is_seller = (True if role in [p2p.Role.SELLER.name, p2p.Role.BUYER_AND_SELLER.name] else False)
        network[curr_port_number] = p2p.P2PNode(id=id,
                                                port_number=curr_port_number,
                                                is_buyer=is_buyer,
                                                is_seller=is_seller,
                                                nodes=node_view_of_network,
                                                shopping_list=node_dict[id]["shopping list"],
                                                selling_list=node_dict[id]["selling list"]
                                                )
    return network


def run_network(network: dict[int, p2p.P2PNode], run_time:int, stop_network:bool=True):
    """
    Given a network of nodes (which is a dict with keys node IDs and values P2PNodes),
    handle set up and then start all nodes running.
    stop_network dictates whether we stop the network after a certain amount of time,
    and run_time dictates how long that time is.
    """
    # Delete leader logs and leaders clocks that are left over from a previous run.
    log_path = Path("leader_log.csv")
    clock_path = Path("leader_clock")
    timestamp_path = Path("node_0_request_timestamps_redone.csv")
    if log_path.exists():
        log_path.unlink()
    if clock_path.exists():
        clock_path.unlink()
    if timestamp_path.exists():
        timestamp_path.unlink()
    time.sleep(1)  # time for logs to be deleted
    # Iterate through and start each node running in its own subprocess.
    process_list = []
    for nid in network.keys():
        node = network[nid]
        p = Process(target=node.start,)
        process_list.append(p)
        p.start()

    if stop_network:
        # If the network is set to stop eventually, wait the specified
        # amount of time, and then send a stop request to each node
        time.sleep(run_time)
        for nid in network.keys():
            node = network[nid]
            port = node.port_number
            msg = dict(type=p2p.ControlMsgType.STOP.name)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as node_socket:
                node_socket.connect((socket.gethostname(), port))
                serialized_msg = pickle.dumps(msg, -1)  # -1 is used to pick best representation
                node_socket.sendall(serialized_msg)
    # By joing the processes, this process won't end until they do.
    # In effect, this means we wait for each node process to end
    for p in process_list:
        p.join()
    return


if __name__ == "__main__":
    num_nodes = int(sys.argv[1])

    if type(num_nodes) is not int or num_nodes < 6:
        print("please pass an integer greater than 5")
        exit(1)
    start_port = 49152
    network = make_random_network(num_nodes=6, start_port=start_port)
    # for
    run_network(network=network, run_time=330)
    pass




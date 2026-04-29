import pickle
import random
import socket
import sys
import time
from multiprocessing import Process
import threading
import enums

import p2p_node as p2p
import warnings

import warehouse_node

warnings.simplefilter(action='ignore', category=FutureWarning)
from pathlib import Path

# Make sure threads fail loudly
def custom_hook(args):
    print(f"Thread failed: {args.exc_type.__name__}: {args.exc_value}")
threading.excepthook = custom_hook

def make_random_network(num_nodes: int, start_port, num_traders:int) -> dict[int, p2p.P2PNode]:
    """
    Creates a random network of nodes, with random roles.
    Returns dict, whose keys are ports and whose values are the nodes.
    """
    # Start by defining the port for each node.
    network = dict()
    node_ports = {i:start_port+i for i in range(0, num_nodes)}
    # Iterate through to initialize all nodes.
    warehouse_port = 0
    for id in range(num_nodes):
        curr_port_number = node_ports[id]
        if id == 0:
            warehouse_port = curr_port_number - 1
            wh_node = warehouse_node.Warehouse(id=num_nodes,
                                               port=warehouse_port,
                                               nodes=node_ports)
            network[warehouse_port] = wh_node
        curr_port_number = node_ports[id]
        role = random.choice(list(enums.Role)).name
        # Give this node a list of all node ports except its own.
        node_view_of_network = node_ports.copy()
        del node_view_of_network[id]
        # Three possible roles, BUYER, SELLER, and BUYER_AND_SELLER. These are passed to
        # the node via the following two bools.
        is_buyer = (True if role in [enums.Role.BUYER.name] else False)
        is_seller = (True if role in [enums.Role.SELLER.name] else False)
        # FIXME: remove debugging statements
        is_buyer = False
        is_seller = True
        network[curr_port_number] = p2p.P2PNode(id=id,
                                                port_number=curr_port_number,
                                                is_buyer=is_buyer,
                                                is_seller=is_seller,
                                                nodes=node_view_of_network,
                                                warehouse_port=warehouse_port,
                                                num_traders = num_traders
                                                )
    return network


def run_network(network: dict[int, p2p.P2PNode], run_time:int, stop_network:bool=True):
    """
    Given a network of nodes (which is a dict with keys node IDs and values P2PNodes),
    handle set up and then start all nodes running.
    stop_network dictates whether we stop the network after a certain amount of time,
    and run_time dictates how long that time is.
    """
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
            msg = dict(type=enums.ControlMsgType.STOP.name)
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
    # num_nodes = int(sys.argv[1])

    # FIXME: remove debug statement
    num_nodes = 2

    # if type(num_nodes) is not int or num_nodes < 6:
    #     print("please pass an integer greater than 5")
    #     exit(1)
    start_port = 49153
    network = make_random_network(num_nodes=num_nodes, start_port=start_port, num_traders=1)
    run_network(network=network, run_time=10000)




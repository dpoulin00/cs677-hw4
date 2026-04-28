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
warnings.simplefilter(action='ignore', category=FutureWarning)
from pathlib import Path

# Make sure threads fail loudly
def custom_hook(args):
    print(f"Thread failed: {args.exc_type.__name__}: {args.exc_value}")
threading.excepthook = custom_hook


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
    num_nodes = int(sys.argv[1])

    if type(num_nodes) is not int or num_nodes < 6:
        print("please pass an integer greater than 5")
        exit(1)
    start_port = 49152
    #network = make_random_network(num_nodes=num_nodes, start_port=start_port)
    # for
    #run_network(network=network, run_time=10000)
    pass




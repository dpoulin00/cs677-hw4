import pickle
import random
import socket
import time
from multiprocessing import Process
import p2p_node as p2p
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from pathlib import Path


def make_random_network(num_nodes: int, start_port) -> dict[int, p2p.P2PNode]:
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
    return dict()


def run_network(network: dict[int, p2p.P2PNode], run_time:int):
    process_list = []
    for nid in network.keys():
        node = network[nid]
        p = Process(target=node.start,)
        process_list.append(p)
        p.start()
    time.sleep(run_time)
    # Iterate through nodes and send each a STOP msg
    for nid in network.keys():
        node = network[nid]
        port = node.port_number
        msg = dict(type=p2p.ControlMsgType.STOP.name)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as node_socket:
            node_socket.connect((socket.gethostname(), port))
            serialized_msg = pickle.dumps(msg, -1)  # -1 is used to pick best representation
            node_socket.sendall(serialized_msg)
            # FIXME: low priority, but it a node is resigned at time of STOP, it might discard the stop.
            # Could set up logic here to resend in such a case.
    # Wait for each node process to end
    for p in process_list:
        p.join()
    return


if __name__ == "__main__":
    start_port = 49152
    log_path = Path("leader_log.csv")
    if log_path.exists():
        log_path.unlink()
    network = make_random_network(num_nodes=5, start_port=start_port)
    run_network(network=network, run_time=10000)
    pass




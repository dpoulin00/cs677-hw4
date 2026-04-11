import pickle
import random
import socket
import time
from multiprocessing import Process
import p2p_node as p2p


def make_random_network(num_nodes: int, start_port) -> dict[int, p2p.P2PNode]:
    """
    Creates a random networ of nodes. Returns dict, whose keys are ports
    and whose values are the nodse.
    """
    network = dict()
    # Iterate through once to initialize all nodes.
    for i in range(num_nodes):
        id = i
        curr_port_number = start_port + i
        role = random.choice(list(p2p.Role)).name
        # Three possible roles, BUYER, SELLER, and BUYER_AND_SELLER
        is_buyer = (True if role in [p2p.Role.BUYER.name, p2p.Role.BUYER_AND_SELLER.name] else False)
        is_seller = (True if role in [p2p.Role.SELLER.name, p2p.Role.BUYER_AND_SELLER.name] else False)
        network[curr_port_number] = p2p.P2PNode(id=id,
                                                port_number=curr_port_number,
                                                is_buyer=is_buyer,
                                                is_seller=is_seller
                                                )
    # Iterate through again to pass network dict to all nodes.
    for port in network.keys():
        node_view_of_network = network.copy()  # Shallow copy so it's same nodes within
        del node_view_of_network[port]  # Remove node from it's own view to avoid msgs to self
        network[port].set_nodes(node_view_of_network)
    return network


def dict_to_network(node_dict: dict) -> dict[int, p2p.P2PNode]:
    return dict()


def run_network(network: dict[int, p2p.P2PNode]):
    process_list = []
    for nid in network.keys():
        node = network[nid]
        p = Process(target=node.start,)
        process_list.append(p)
        p.start()
    time.sleep(10)
    # Iterate through nodes and send each a STOP msg
    for nid in network.keys():
        node = network[nid]
        port = node.port_number
        msg = dict(type=p2p.ControlMsgType.STOP.name)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as node_socket:
            node_socket.connect((socket.gethostname(), port))
            serialized_msg = pickle.dumps(msg, -1)  # -1 is used to pick best representation
            node_socket.sendall(serialized_msg)
    # Wait for each node process to end
    for p in process_list:
        p.join()
    return


if __name__ == "__main__":
    start_port = 49152
    network = make_random_network(num_nodes=10, start_port=start_port)
    run_network(network=network)
    pass




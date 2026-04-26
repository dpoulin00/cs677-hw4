import warehouse_node
import socket
from multiprocessing import Process
import enums
import uuid
import pickle
from concurrent.futures.thread import ThreadPoolExecutor


def warehouse_test1_buy(use_process=False):
    # Set up warehouse node
    test_id = 0
    test_port = 49152
    wh_node = warehouse_node.Warehouse(id=1,
                                       port=49153,
                                       nodes={test_id: test_port})
    with ThreadPoolExecutor(max_workers=100) as executor:
        if use_process:
            p = Process(target=wh_node.start,)
            p.start()
        else:
            executor.submit(wh_node.start)
        # Set up a socket for warehouse node to send to
        test_socket = socket.socket()
        test_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        test_socket.settimeout(100)  # Time out so we can gracefully exit once we stop seeing messages.
        test_socket.bind((socket.gethostname(), test_port))
        test_socket.listen(100000)
        # Send a buy request to the warehouse node
        msg = enums.TxMsg(uid=uuid.uuid4(),
                        sender=test_id,
                        type=enums.MsgType.BUY.name,
                        item=enums.Item.SALT.name,
                        quantity=5).to_dict()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as node_socket:
            dest_port = 49153
            node_socket.connect((socket.gethostname(), dest_port))
            serialized_msg = pickle.dumps(msg, -1)  # -1 is used to pick best representation
            node_socket.sendall(serialized_msg)
            node_socket.close()
        # Receive reply from warehouse
        socket_connection, addr = test_socket.accept()
        data = socket_connection.recv(4096)
        socket_connection.close()
        msg = pickle.loads(data)
        pass
    return



if __name__ == "__main__":
    warehouse_test1_buy(use_process=False)



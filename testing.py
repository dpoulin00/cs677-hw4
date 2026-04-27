import warehouse_node
import socket
from multiprocessing import Process
import enums
import uuid
import pickle
from concurrent.futures.thread import ThreadPoolExecutor
import time
import unittest


def send_msg(msg:dict, dest_port:int):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as node_socket:
        dest_port = 49153
        time.sleep(5)
        node_socket.connect((socket.gethostname(), dest_port))
        serialized_msg = pickle.dumps(msg, -1)  # -1 is used to pick best representation
        node_socket.sendall(serialized_msg)
        node_socket.close()
    return

def recieve_msg(my_socket:socket.socket):
    socket_connection, addr = my_socket.accept()
    data = socket_connection.recv(4096)
    socket_connection.close()
    msg = pickle.loads(data)
    return msg


class TestWarehouse(unittest.TestCase):
    def setUp(self):
        # Set up warehouse node
        self.test_id = 0
        self.test_port = 49152
        self.wh_port = 49153
        wh_node = warehouse_node.Warehouse(id=1,
                                           port=self.wh_port,
                                           nodes={self.test_id: self.test_port})
        # Set up test socket for warehouse node to send msgs to
        self.socket = socket.socket()
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.settimeout(100)  # Time out so we can gracefully exit once we stop seeing messages.
        self.socket.bind((socket.gethostname(), self.test_port))
        self.socket.listen(100000)
        # set up thread testing
        use_process = False            
        self.thread_executor = ThreadPoolExecutor(max_workers=100)
        if use_process:
            p = Process(target=wh_node.start,)
            p.start()
        else:
            self.thread_executor.submit(wh_node.start)
        # Wait a few seconds so warehouse has time to start
        time.sleep(5)
        return
    
    def tearDown(self):
        stop_msg = dict(type=enums.ControlMsgType.STOP.name)
        send_msg(stop_msg, dest_port=self.wh_port)
        self.socket.close()
        self.thread_executor.shutdown()
        return
    
    def test_buy(self):
        # Send a buy request to the warehouse node
        uid = uid=uuid.uuid4()
        outgoing_msg = enums.TxMsg(uid=uid,
                                   sender=self.test_id,
                                   type=enums.MsgType.BUY.name,
                                   item=enums.Item.SALT.name,
                                   quantity=5).to_dict()
        send_msg(outgoing_msg, dest_port=self.wh_port)
        # Receive reply from warehouse
        msg = recieve_msg(self.socket)
        expected_msg = dict(uid=uid,
                            sender=1,
                            type=enums.MsgType.BUY_REPLY.name,
                            item=enums.Item.SALT.name,
                            quantity=0)
        self.assertDictEqual(msg, expected_msg)
        return
    
    def test_restock(self):
        # Send a restock request to the warehouse node
        uid = uid=uuid.uuid4()
        outgoing_msg = enums.TxMsg(uid=uid,
                                   sender=self.test_id,
                                   type=enums.MsgType.RESTOCK.name,
                                   item=enums.Item.SALT.name,
                                   quantity=5).to_dict()
        send_msg(outgoing_msg, dest_port=self.wh_port)
        # Receive reply from warehouse
        msg = recieve_msg(self.socket)
        expected_msg = dict(uid=uid,
                            sender=1,
                            type=enums.MsgType.RESTOCK_REPLY.name,
                            item=enums.Item.SALT.name,
                            quantity=5)
        self.assertDictEqual(msg, expected_msg)
        return


if __name__ == "__main__":
    unittest.main()
    #warehouse_test1_buy(use_process=True)



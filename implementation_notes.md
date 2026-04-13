
Life of a node:
1. A node starts running and initiates an election. See life of an election.
2. Assume this node is not a leader (see life of a leader). This node will iterate through the run_loop, starting BUY and RESTOCK transactions to buy items and add new items to the store after certain intervals.
3. Each BUY and RESTOCK msg is sent, and corresponding records are added to the node_log. Initial status of these records is STARTED.
4. Nodes expect the leader to send an ACK back for each BUY and RESTOCK msg. Once these are received, status of corresponding record is updated to ACKED.
- - If no ACK is received, we interpret that as the node being down. The record status is changed to NEEDS_RESEND (to be resent after a new leader is picked), and an election is started.
5. Once a finalization msg is received from the leader, the corresponding record in the log is marked as DONE.
6. To keep track of these details, each run_loop, the node reviews the node_log to check on the status of transactions.

Life of an Election:
1. We use the bully algorithm. Whenever a node spawns an election with self.elect(), an election record is added to the node_log. Initial status is STARTED.
2. If we receive an OKAY, status is updated to DONE.
3. In run_loop, we keep track of how long it has been since an election was started.
- - After a while, if the election still isn't DONE, we declare victory and send out an IWON.
- - If there is no ongoing election, and it's been a while since the last one ended, we start a new one.
4. Once we receive an IWON, we record the new leader and go back to buying and selling.

Life of a leader:
1. When becoming a leader, a node sends out an IWON, and then picks up the log.
2. A leader just accepts msgs and responds accordingly. Each of these are added to the log.
3. After a certain interval, the leader will resign.
4. If a leader receives an IWON, it resigns.
5. When resigning, the leader saves the log.

Life of a transaction:
1. Created at client node and added to node_log.
2. Sent to leader. Added to leader_log and an ACK is sent back.
3. Leader waits for the vector clock logic, and then processes the transaction.



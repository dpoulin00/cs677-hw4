# Election Logic
- Nodes need only be elected once, when the program starts
- Network originally initialized with two types of nodes, peers and DB 
- In the case that we cannot specify which nodes are traders beforehand and they HAVE to be elected, use the following procedure
- Possible election strategies:
  - Complicated but maybe required approach: use standard bully election algorithm as previously implemented
    - When program starts, use bully election to elect the first coordinator
    - After first coordinator is elected, use repeat bully election again until all coordinators are elected
      - This will require us to implement a new message type distinct from IWON sent by an elected leader which states it is already the leader
  - Simple approach which may exceed the bounds of the assignment: 
    - every node has a list of peers, simply check if that peer is in the top $N_t$ nodes. If it is it is a leader, otherwise it is a peer.
    - Non-leader nodes then pick a leader at random (or on a per-request basis, requirement in assignment description unclear)
    - requires no network communication
# Synchronized Approach to requests
## Purchasing Logic
- Nodes may make a request to purchase an item at any time
- Nodes forward this request to the trader who then forwards it to the database store as received. Additionally, the trader responds to the peer indicating the request went through.
- Database store checks requests as they are received
- Database store reacts accordingly to the rules outlined in the assignment
- Database store responds to leader as to whether or not the buy was successful or not
## Selling logic
- Nodes sell $N_g$ items to the trader every $T_g$ seconds.
  - Note: Per the assignment description, rather than selling to peers, they are sold to the trader. 
- The trader forwards these items to the database process, which stores this data in the database. Additionally, the leader responds to the peer indicating the request went through.
- Database process replies to trader stating that these items were successfully stored in the warehouse
# Inventory Information Cache Approach
- Potentially a good idea to implement Eventual Consistency, below approach uses that method
## Purchasing Logic
- Nodes may make a request to purchase an item at any time
- Nodes forward this request to the trader
- Each trader maintains a vector clock used for ordering both buys and sells. The indices in the vector clock represent each trader
- Upon receiving the request, the trader updates its vector clock, and multicasts the clock update to all other traders. Each trader increments its clock accordingly
- Upon receiving the request, the trader forwards it to the database store if and only if the local datastore deems it okay. Additionally, it responds to the peer who made the request indicating the request went through.
- The database store processes requests in the order according to its own vector clock which it synchronizes with the request vector clock according to the standard update rules, requests are stored in the same way requests were stored for HW4. Since we no longer have the issue of leader nodes temporarily going down and the vector clock is maintained at the leader level, desyncs we encountered last time should NOT be an issue
- If an update was unsuccessful, respond to the trader indicating as such
- If an update was successful, respond to ALL traders indicating that it was, update the local datastore copies accordingly.
- Trader responds to the database process indicating it was successfully received
- ## Selling Logic
- Nodes may make a request to sell an item every $T_g$ seconds.
  - Note: per the assignment description, goods are sold to the trader and not to the buyer
- Each trader maintains a vector clock used for ordering both buys and sells. The indices in the vector clock represent each trader
- Upon receiving the request, the trader updates its vector clock, and multicasts the clock update to all other traders. Each trader increments its clock accordingly
- Upon receiving the request, the trader forwards it to the database store if and only if the local datastore deems it okay. Additionally, it responds to the peer who made the request indicating the request went through.
- The database store processes requests in the order according to its own vector clock which it synchronizes with the request vector clock according to the standard update rules, requests are stored in the same way requests were stored for HW4. Since we no longer have the issue of leader nodes temporarily going down and the vector clock is maintained at the leader level, desyncs we encountered last time should NOT be an issue
- If an update was unsuccessful, respond to the trader indicating as such
- if an update was successful, respond to ALL traders indicating that it was, update the local datastore copies accordingly.
# Fault Tolerance Logic
- NOTE: Under fault tolerance, our system only needs to support two leader nodes, thus the following logic assumes this to be the case.
- Leaders send a heartbeat message to each other every x seconds, and respond to each other upon receiving the message indicating that it was successful.
- After y seconds, if a leader has not heard back, that leader becomes the sole leader. 
- This leader then sends a message to all other nodes, both the peers and the database process indicating it is now the sole trader.
- Messages that haven't received a response from a previous trader are now forwarded to this trader. This includes:
  - Requests sent by peers making buy and sell requests not acknowledged by the trader
  - The request sent by the database process indicating that a buy was successful or an item was successfully stocked
    - Note: This is why we handle the vector clock on the trader level, to avoid issues with the trader going down. 
# Implementation Details
- To be filled out in more detail later. I think we can use a similar approach used to HW4 with the listen loop approach and similar composition into standard components.

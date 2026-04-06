Code Principles:
1. Let's use enums for string consistency. However, they don't always play nice with pickling, so let's use the .name attribute.
    e.g. we could have "if msg["msg_type"] == msg_types.ELC_RESP.name:"
2. Let's use typing in all function signatures. This makes it easier to keep track of variables.
3. Let's indicate when a function is used in the docstring. E.g. "Called by buyer when making a new purchases, sends message to trader."


Node design:
Let's use one running loop this time.  This will
1. Check if any messages have been received (we'll need to not block at socket listening this time),
2. Submit buy requests after a certain amount of time (we'll need to not block between buy requests).
Other improvements over last time:
1. The parent process that starts nodes running should be able to shut them down to facilitate graceful exits. This
    will require it sending messages to each node to disable the self.running parameter to exit the running loop.
2. To facilitate leader resignations, let's also have a self.resigned parameter.
    The running loop will thus be (while self.running: while self.running and not self.resigned).
Differences in requirements from last time:
1. Nodes can sometimes be both a buyer and a seller
2. Buyers can buy mutiple items at once.

Message Types:
1. buy_init: sent from buyer to leader to initiate buy request (multicast, increments clocks).
2. buy_resp: sent from leader to buyer to either say "buy worked" or "buy failed" (unicast, no increment).
3. buy_pmnt: sent from leader to seller to represent payment after selling an item (unicast, no increment).
4. upd_invn: sent from seller to leader when seller picks a new item to stock (unicast, increment). ()

5. elc_rsin: Sent by leader when they resign (multicast, increment).
6. elc_elec: Sent by node x to all nodes with higher IDs to see if they're alive (multicast to higher IDs only, no increment).
7. elc_okay: Resonse to elc_elec which tells elc_elec sender a node with higher id is alive (unicast, no increment).
8. elc_iwon: Sent by node if it receives no response to elc_poll to tell all other nodes it is newest trader (multicast, no increment).

9. msg_recv: Sent in response to certain messages once they're received and processed. This is to avoid messages getting lsot when a leader resigns. If we dont get once
    after a certain interval, resend to current leader. If no leader, wait until there is and resend.


Leader Retirement:
Retire with some small random probability each loop. Should be very small, e.g. 0.001, since we anticipate going through loops very quickly.

Leader Election:
A bully algorithm seems easiest to implement. To make this actually interesting, when a leader resigns, we'll want to make
them unavailable for some random amount of time. They should not send or receive messages during this time. We'll need to make sure
the socket queue doesn't fill up somehow. When they come back online, we shouldn't need to do anything special other than start them running again (thanks to how vector clocks work).
I'd recommend rather than blocking to do this, we temporarily disable the running variable.

Since "we can assume that the last coordinator will write all the useful information to a file so that the new coordinator can read such information from the file,"
I think it's fair to have leader finish parsing



Leader Data:
Rather than printing to logs, we'll want to open and close files. We'll also have to make sure the data is actually being written, such that the next
trader can actually see it.
I would recommend the trader keeps track of data via a pandas dataframe, which can then be written to a csv when the trader retires.
A new trader need only read the csv to get all info on the previous trader's state.

Note that needed data is (number of goods available for sale, pending sales). I recommend having
self.sales = df with all pending sales included. Columns include sale id, buyer, item, seller, clock state, isfinished boolean.
self.inventory = df with columns including item, clock received, seller. So if we ahve 5 fish in stock, we'd have five rows for fish. (This is absed on my current understanding, may change with piazza post)


Buying Process:



Vector clock implementation (page 266 of textbook, 270-271 for example):
    When we receive a message, we pass it to the processing function, which delays proceeding with it until the Vector Clock algorithm say we can.
    This will require repeatedly checking the node's clock.
    Note that all messages (or at least all initial requests) will need to be multicast, such that the clocks of eahc node can be updated.

    In order to avoid processing a request before the other request has finished, when we detect a request is the next logical request, we'll want to
        1. lock the processing code,
        2. increment the clock,
        3. process the request (decrementing the item counter),
        4. and then unlock the processing code.




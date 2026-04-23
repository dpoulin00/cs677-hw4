import main
import p2p_node as p2p



def test_no_sellers():
    """
    We set up a network with no sellers.
    This should result in every buy request failing.
    """
    node_dict = {
        0: {"port": 49152,
            "role": p2p.Role.BUYER.name,
            "shopping list": [{p2p.Item.BOAR.name: 10}]*1000,
            "selling list": []},
        1: {"port": 49153,
            "role": p2p.Role.BUYER.name,
            "shopping list": [{p2p.Item.BOAR.name: 10}]*1000,
            "selling list": []},
        2: {"port": 49154,
            "role": p2p.Role.BUYER.name,
            "shopping list": [{p2p.Item.BOAR.name: 10}]*1000,
            "selling list": []},
        3: {"port": 49155,
            "role": p2p.Role.BUYER.name,
            "shopping list": [{p2p.Item.BOAR.name: 10}]*1000,
            "selling list": []},
        4: {"port": 49156,
            "role": p2p.Role.BUYER.name,
            "shopping list": [{p2p.Item.BOAR.name: 10}]*1000,
            "selling list": []},
        5: {"port": 49157,
            "role": p2p.Role.BUYER.name,
            "shopping list": [{p2p.Item.BOAR.name: 10}]*1000,
            "selling list": []},
        6: {"port": 49158,
            "role": p2p.Role.BUYER.name,
            "shopping list": [{p2p.Item.BOAR.name: 10}]*1000,
            "selling list": []},
            
    }
    network = main.dict_to_network(node_dict=node_dict)
    main.run_network(network=network, run_time=100)
    return

def test_no_buyers():
    """
    We set up a network with no buyers.
    This should result in only restocks being started.
    """
    node_dict = {
        0: {"port": 49152,
            "role": p2p.Role.SELLER.name,
            "shopping list": [{p2p.Item.BOAR.name: 10}]*1000,
            "selling list": [{p2p.Item.BOAR.name: 10}]*1000,},
        1: {"port": 49153,
            "role": p2p.Role.SELLER.name,
            "shopping list": [{p2p.Item.BOAR.name: 10}]*1000,
            "selling list": [{p2p.Item.BOAR.name: 10}]*1000,},
        2: {"port": 49154,
            "role": p2p.Role.SELLER.name,
            "shopping list": [{p2p.Item.BOAR.name: 10}]*1000,
            "selling list": [{p2p.Item.BOAR.name: 10}]*1000,},
        3: {"port": 49155,
            "role": p2p.Role.SELLER.name,
            "shopping list": [{p2p.Item.BOAR.name: 10}]*1000,
            "selling list": [{p2p.Item.BOAR.name: 10}]*1000,},
        4: {"port": 49156,
            "role": p2p.Role.SELLER.name,
            "shopping list": [{p2p.Item.BOAR.name: 10}]*1000,
            "selling list": [{p2p.Item.BOAR.name: 10}]*1000,},
        5: {"port": 49157,
            "role": p2p.Role.SELLER.name,
            "shopping list": [{p2p.Item.BOAR.name: 10}]*1000,
            "selling list": [{p2p.Item.BOAR.name: 10}]*1000,},
        6: {"port": 49158,
            "role": p2p.Role.SELLER.name,
            "shopping list": [{p2p.Item.BOAR.name: 10}]*1000,
            "selling list": [{p2p.Item.BOAR.name: 10}]*1000,},
            
    }
    network = main.dict_to_network(node_dict=node_dict)
    main.run_network(network=network, run_time=100)
    return

def test_no_salt_sellers():
    """
    In this test, we have salt buyers, but no salt sellers.
    All salt buy request should fail, but other buy requests may succeed.
    Note that initial buy requests of any item may fail, as the restock requests may not
    have gone through yet.
    """
    node_dict = {
        0: {"port": 49152,
            "role": p2p.Role.SELLER.name,
            "shopping list": [{p2p.Item.BOAR.name: 10}]*1000,
            "selling list": [{p2p.Item.BOAR.name: 10}]*1000,},
        1: {"port": 49153,
            "role": p2p.Role.BUYER.name,
            "shopping list": [{p2p.Item.BOAR.name: 10}]*1000,
            "selling list": [{p2p.Item.BOAR.name: 10}]*1000,},
        2: {"port": 49154,
            "role": p2p.Role.BUYER_AND_SELLER.name,
            "shopping list": [{p2p.Item.SALT.name: 10}]*1000,
            "selling list": [{p2p.Item.FISH.name: 10}]*1000,},
        3: {"port": 49155,
            "role": p2p.Role.BUYER.name,
            "shopping list": [{p2p.Item.FISH.name: 10}]*1000,
            "selling list": [{p2p.Item.FISH.name: 10}]*1000,},
        4: {"port": 49156,
            "role": p2p.Role.BUYER_AND_SELLER.name,
            "shopping list": [{p2p.Item.FISH.name: 10}]*1000,
            "selling list": [{p2p.Item.BOAR.name: 10}]*1000,},
        5: {"port": 49157,
            "role": p2p.Role.SELLER.name,
            "shopping list": [{p2p.Item.SALT.name: 10}]*1000,
            "selling list": [{p2p.Item.BOAR.name: 10}]*1000,},
        6: {"port": 49158,
            "role": p2p.Role.SELLER.name,
            "shopping list": [{p2p.Item.SALT.name: 10}]*1000,
            "selling list": [{p2p.Item.BOAR.name: 10}]*1000,},
            
    }
    network = main.dict_to_network(node_dict=node_dict)
    main.run_network(network=network, run_time=100)
    return

def test_not_enough_boar():
    """
    Create a case where more board is requested than is available.
    We expect to see only one board request, and only 10 board, not the
    11 requested, should be sold.
    Note that the boar is requested only after several other requests,
    such that we can be sure the boar is purchased after it is stock.
    """
    node_dict = {
        0: {"port": 49152,
            "role": p2p.Role.SELLER.name,
            "shopping list": [{p2p.Item.FISH.name: 10}]*1000,
            "selling list": [{p2p.Item.BOAR.name: 10}] + [{p2p.Item.FISH.name: 10}]*1000,},
        1: {"port": 49153,
            "role": p2p.Role.BUYER.name,
            "shopping list": [{p2p.Item.FISH.name: 10}]*10 + [{p2p.Item.BOAR.name: 11}] + [{p2p.Item.FISH.name: 10}]*1000,
            "selling list": [{p2p.Item.FISH.name: 10}]*1000,},
        2: {"port": 49154,
            "role": p2p.Role.BUYER_AND_SELLER.name,
            "shopping list": [{p2p.Item.SALT.name: 10}]*1000,
            "selling list": [{p2p.Item.FISH.name: 10}]*1000,},
        3: {"port": 49155,
            "role": p2p.Role.BUYER.name,
            "shopping list": [{p2p.Item.FISH.name: 10}]*1000,
            "selling list": [{p2p.Item.FISH.name: 10}]*1000,},
        4: {"port": 49156,
            "role": p2p.Role.BUYER_AND_SELLER.name,
            "shopping list": [{p2p.Item.FISH.name: 10}]*1000,
            "selling list": [{p2p.Item.FISH.name: 10}]*1000,},
        5: {"port": 49157,
            "role": p2p.Role.SELLER.name,
            "shopping list": [{p2p.Item.SALT.name: 10}]*1000,
            "selling list": [{p2p.Item.FISH.name: 10}]*1000,},
        6: {"port": 49158,
            "role": p2p.Role.SELLER.name,
            "shopping list": [{p2p.Item.SALT.name: 10}]*1000,
            "selling list": [{p2p.Item.FISH.name: 10}]*1000,},
            
    }
    network = main.dict_to_network(node_dict=node_dict)
    main.run_network(network=network, run_time=100)
    return

def test_buyer_and_seller_does_both():
    """
    In this case, we want to make sure a ndoe that is both a
    buyer and a seller des both. To that end, we have one node
    that is a buyer and a seller, which buys BOAR and sells FISH.
    We have 3 nodes that only buy and only buy FISH,
    and 3 nodes that only sell and only sell fish.
    We expect to see FISH and BOAR purchases go through, which
    will indicate the node that is both a buyer and seller is
    indeed both buying and selling.
    Note many FISH purchases will fail, as we have more buyers than
    sellers, so sometimes a buyer will attempt to buy a fish
    when there is no FISH available.
    """
    node_dict = {
        0: {"port": 49152,
            "role": p2p.Role.BUYER_AND_SELLER.name,
            "shopping list": [{p2p.Item.BOAR.name: 10}]*1000,
            "selling list": [{p2p.Item.FISH.name: 10}]*1000,},
        1: {"port": 49153,
            "role": p2p.Role.BUYER.name,
            "shopping list": [{p2p.Item.FISH.name: 10}]*1000,
            "selling list": []},
        2: {"port": 49154,
            "role": p2p.Role.BUYER.name,
            "shopping list": [{p2p.Item.FISH.name: 10}]*1000,
            "selling list": [],},
        3: {"port": 49155,
            "role": p2p.Role.BUYER.name,
            "shopping list": [{p2p.Item.FISH.name: 10}]*1000,
            "selling list": [],},
        4: {"port": 49156,
            "role": p2p.Role.SELLER.name,
            "shopping list": [],
            "selling list": [{p2p.Item.BOAR.name: 10}]*1000,},
        5: {"port": 49157,
            "role": p2p.Role.SELLER.name,
            "shopping list": [],
            "selling list": [{p2p.Item.BOAR.name: 10}]*1000},
        6: {"port": 49158,
            "role": p2p.Role.SELLER.name,
            "shopping list": [],
            "selling list": [{p2p.Item.BOAR.name: 10}]*1000},
            
    }
    network = main.dict_to_network(node_dict=node_dict)
    main.run_network(network=network, run_time=150)
    return

def test_elections_work():
    """
    Rather than defining a network, this test
    uses a random network, but a longer runtime than our other
    tests. THis is so we can see if, after nodes resign, elections
    function properly, according to the bully algorithm.

    We expect, as different nodes become leader, for transactions to continue.
    Further, we expect first for node 9 to be the leader, and then for node 8 to
    become leader when node 9 resigns (after node 8 wins teh election). As soon
    as node 9 comes back online, it should start and win an election to become leader,
    but if node 8 resigns while node 9 is still down, node 7 should start and win an
    election.
    """
    network = main.make_random_network(num_nodes=10, start_port=49152)
    main.run_network(network=network, run_time=10000)
    return

def test_payment_amounts_are_correct():
    """
    The price for each product is predefined as
    1 for SALT, 5 for FISH, and 10 for BOAR.
    This function creates a network with 1 seller and 1
    buyer of each product (and an extra node to be initial leader),
    such that we can check that the purchase amounts
    going through are correct, i.e., that they equal price of item * quantity sold.
    """
    node_dict = {
        0: {"port": 49152,
            "role": p2p.Role.SELLER.name,
            "shopping list": [],
            "selling list": [{p2p.Item.BOAR.name: 1}]*1000,},
        1: {"port": 49153,
            "role": p2p.Role.SELLER.name,
            "shopping list": [],
            "selling list": [{p2p.Item.FISH.name: 1}]*1000,},
        2: {"port": 49154,
            "role": p2p.Role.SELLER.name,
            "shopping list": [],
            "selling list": [{p2p.Item.SALT.name: 1}]*1000,},
        3: {"port": 49155,
            "role": p2p.Role.BUYER.name,
            "shopping list": [{p2p.Item.BOAR.name: 1}]*1000,
            "selling list": [],},
        4: {"port": 49156,
            "role": p2p.Role.BUYER.name,
            "shopping list": [{p2p.Item.FISH.name: 1}]*1000,
            "selling list": [],},
        5: {"port": 49157,
            "role": p2p.Role.BUYER.name,
            "shopping list": [{p2p.Item.SALT.name: 1}]*1000,
            "selling list": [],},  
        6: {"port": 49158,
            "role": p2p.Role.BUYER.name,
            "shopping list": [{p2p.Item.SALT.name: 1}]*1000,
            "selling list": [],},   
    }
    network = main.dict_to_network(node_dict=node_dict)
    main.run_network(network=network, run_time=500)
    return

def test_6_nodes():
    """
    Test that network functions with 6 nodes (ie, transactions go through
    and leaders are elected)
    """
    network = main.make_random_network(num_nodes=6, start_port=49152)
    main.run_network(network=network, run_time=500)
    return


def test_10_nodes():
    """
    Test that network functions with 10 nodes (ie, transactions go through
    and leaders are elected)
    """
    network = main.make_random_network(num_nodes=10, start_port=49152)
    main.run_network(network=network, run_time=500)
    return



if __name__ == "__main__":
    # Note: all tests were run and passed. While the STOP command sent by the main process when the program
    # closes sometimes generates an error, a graceful exit is outside of the homework requirements (we could
    # just as easily remove the STOP message altogether, and leave it to the user to close the terminal when
    # theyre done).

    # To choose a test, select a number for test_num. Again, we ran all tests.
    
    test_num = 1
    match test_num:
        case 1:
            test_no_sellers()
        case 2:
            test_no_buyers()
        case 3:
            test_no_salt_sellers()
        case 4:
            test_not_enough_boar()
        case 5:
            test_buyer_and_seller_does_both()
        case 6:
            test_elections_work()
        case 7:
            test_payment_amounts_are_correct()
        case 8:
            test_6_nodes()
        case 9:
            test_10_nodes()



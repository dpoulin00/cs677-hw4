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
    return

def test_no_salt_sellers():
    return

def test_not_enough_boar():
    return

def test_6_nodes():
    return

def test_100_nodes():
    return



if __name__ == "__main__":
    test_no_sellers()



import requests
import json
from collections import Counter
from tabulate import tabulate
import click
from joblib import Memory, Parallel, delayed
from brownie import Contract, chain, web3
from web3.middleware.filter import block_ranges
from toolz import concat, unique, valmap, keymap
from tqdm import tqdm
from eth_utils import event_abi_to_log_topic, encode_hex, to_checksum_address
from collections import defaultdict

memory = Memory('cache', verbose=0)
ANCIENT_POOLS = {
    '0x0001FB050Fe7312791bF6475b96569D83F695C9f': 1,  # ycrv
    '0x033E52f513F9B98e129381c6708F9faA2DEE5db5': 2,  # yfi/dai bpt
    '0x3A22dF48d84957F907e67F4313E3D43179040d6E': 3,  # yfi/ycrv bpt, governance v1
    '0xb01419E74D8a2abb1bbAD82925b19c36C191A701': 4,  # rewards
    '0xBa37B002AbaFDd8E89a1995dA52740bbC013D992': 5,  # governance v2
}
EARLIEST_BLOCK = 10_476_729
YGIFT_DEPLOY_BLOCK = 11_287_863
YEARN_COORDINAPE_CIRCLES = {1, 2, 3}


@memory.cache()
def get_logs(address, topics, from_block, to_block):
    return web3.eth.get_logs(
        {
            "address": address,
            "topics": topics,
            "fromBlock": from_block,
            "toBlock": to_block,
        }
    )


def get_logs_batched(address, topics, from_block, to_block, batch_size=10_000):
    blocks = list(block_ranges(from_block, to_block, batch_size))

    yield from concat(
        tqdm(
            Parallel(8, 'threading')(
                delayed(get_logs)(address, topics, start, end) for start, end in blocks
            ),
            total=len(blocks),
        )
    )


@memory.cache()
def get_snapshot_voters():
    query = '''{ votes(first: 100000, where: {space_in: ["yearn", "ybaby.eth"]}) { voter } }'''
    response = requests.post('https://hub.snapshot.page/graphql', json={'query': query})

    return set(vote['voter'] for vote in response.json()['data']['votes'])


@memory.cache()
def get_coordinape_users():
    users = requests.get('https://coordinape.me/api/users').json()

    return {
        to_checksum_address(user['address'])
        for user in users
        if user['circle_id'] in YEARN_COORDINAPE_CIRCLES
    }


def get_ygift_users():
    users = defaultdict(set)
    ygift = Contract('0x020171085bcd43b6FD36aD8C95aD61848B1211A2')
    contract = web3.eth.contract(str(ygift), abi=ygift.abi)
    gift_minted = contract.events.GiftMinted()
    topics = [encode_hex(event_abi_to_log_topic(gift_minted.abi))]
    for log in get_logs_batched(str(ygift), topics, YGIFT_DEPLOY_BLOCK, chain.height):
        event = gift_minted.processLog(log)
        users['senders'].add(event.args['from'])
        users['receivers'].add(event.args['to'])

    return users


def get_ancient_pool_stakers():
    stakers = defaultdict(set)
    pool = Contract(list(ANCIENT_POOLS)[0])
    contract = web3.eth.contract(str(pool), abi=pool.abi)
    staked = contract.events.Staked()
    topics = [encode_hex(event_abi_to_log_topic(staked.abi))]

    for log in get_logs_batched(
        list(ANCIENT_POOLS), topics, EARLIEST_BLOCK, chain.height
    ):
        event = staked.processLog(log)
        stakers[event.address].add(event.args['user'])

    return stakers


def main():
    # 1. voters on snapshot.org
    snapshot_voters = get_snapshot_voters()
    print(len(snapshot_voters), 'snapshot voters')

    # 2. yearn coordinape circles
    coordinape_users = get_coordinape_users()
    print(len(coordinape_users), 'users from coorinape circles')

    # 3. ygift senders and recipients
    ygift = get_ygift_users()
    ygift_users = set(concat(ygift.values()))
    print(len(ygift_users), 'ygift users')

    # 4. stakers in ancient pools
    ancient_pools = get_ancient_pool_stakers()
    pools = keymap(ANCIENT_POOLS.get, ancient_pools)
    print(len(set(concat(pools.values()))), 'ancient pool stakers')
    print(valmap(len, pools))

    days = {
        1: pools[1],
        2: pools[2],
        3: pools[3],
        4: pools[4],
        5: pools[5],
        6: snapshot_voters,
        7: coordinape_users | ygift_users,
    }

    print(valmap(len, days))

    with open('blue-pill.json', 'wt') as f:
        json.dump(valmap(sorted, days), f, indent=2)

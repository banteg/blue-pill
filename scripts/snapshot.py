import json
from collections import defaultdict
from functools import reduce
from itertools import combinations

import requests
from brownie import Contract, chain, web3
from eth_utils import encode_hex, event_abi_to_log_topic, to_checksum_address
from joblib import Memory, Parallel, delayed
from toolz import concat, keymap, valmap
from tqdm import tqdm
from web3.middleware.filter import block_ranges

memory = Memory('cache', verbose=0)

ANCIENT_POOLS = {
    '0x0001FB050Fe7312791bF6475b96569D83F695C9f': 'ycrv',
    '0x033E52f513F9B98e129381c6708F9faA2DEE5db5': 'yfi/dai',
    '0x3A22dF48d84957F907e67F4313E3D43179040d6E': 'yfi/ycrv',
    '0xb01419E74D8a2abb1bbAD82925b19c36C191A701': 'rewards',
    '0xBa37B002AbaFDd8E89a1995dA52740bbC013D992': 'ygov',
}
EARLIEST_BLOCK = 10_476_729
SNAPSHOT_BLOCK = 12_843_076  # yfi deploy + 365 days
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
    query = '''{ votes(first: 100000, where: {space_in: ["yearn", "ybaby.eth"]}) { voter created } }'''
    response = requests.post('https://hub.snapshot.page/graphql', json={'query': query})
    cutoff_timestamp = chain[SNAPSHOT_BLOCK].timestamp

    return set(
        vote['voter']
        for vote in response.json()['data']['votes']
        if vote['created'] <= cutoff_timestamp
    )


@memory.cache()
def get_coordinape_users():
    users = requests.get('https://coordinape.me/api/users').json()

    return {
        to_checksum_address(user['address'])
        for user in users
        if user['circle_id'] in YEARN_COORDINAPE_CIRCLES
    }


def get_ygift_users():
    users = set()
    ygift = Contract('0x020171085bcd43b6FD36aD8C95aD61848B1211A2')
    contract = web3.eth.contract(str(ygift), abi=ygift.abi)
    gift_minted = contract.events.GiftMinted()
    topics = [encode_hex(event_abi_to_log_topic(gift_minted.abi))]
    for log in get_logs_batched(str(ygift), topics, YGIFT_DEPLOY_BLOCK, SNAPSHOT_BLOCK):
        event = gift_minted.processLog(log)
        users.add(event.args['from'])
        users.add(event.args['to'])

    return users


def get_ancient_pool_stakers():
    stakers = defaultdict(set)
    pool = Contract(list(ANCIENT_POOLS)[0])
    contract = web3.eth.contract(str(pool), abi=pool.abi)
    staked = contract.events.Staked()
    topics = [encode_hex(event_abi_to_log_topic(staked.abi))]

    for log in get_logs_batched(
        list(ANCIENT_POOLS), topics, EARLIEST_BLOCK, SNAPSHOT_BLOCK
    ):
        event = staked.processLog(log)
        stakers[event.address].add(event.args['user'])

    return stakers


def intersect_union(data, num):
    intersections = [
        reduce(set.intersection, [set(data[key]) for key in keys])
        for keys in combinations(data, num)
    ]

    return reduce(set.union, intersections)


def main():
    snapshot_voters = get_snapshot_voters()
    coordinape_users = get_coordinape_users()
    ygift_users = get_ygift_users()
    ancient_pools = get_ancient_pool_stakers()
    pools = keymap(ANCIENT_POOLS.get, ancient_pools)

    common = {
        "01 The Farmer": pools['ycrv'] | pools['yfi/dai'] | pools['rewards'],
        "02 The Staker": pools['yfi/ycrv'] | pools['ygov'],
        "03 The Voter": snapshot_voters,
        "04 The Giver": coordinape_users | ygift_users,
    }
    rare = {
        "05 The Lunar Guild": intersect_union(common, 2),
        "06 The Sun's Work": intersect_union(common, 3),
        "07 The Celestial Sphere": intersect_union(common, 4),
    }
    common.update(rare)

    for key in common:
        print(f'{key:24} {len(common[key])}')

    with open('blue-pill.json', 'wt') as f:
        json.dump(valmap(sorted, common), f, indent=2)

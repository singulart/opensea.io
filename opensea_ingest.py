# Usage python3 opensea.py /Users/o.buistov/projects/crypto/opensea-analytics/new/*.json cryptopunk.csv
import json
import glob
from datetime import datetime
import sys
import requests
import math
from db_models import *

eth = 1e18
FORMAT_MAIN = '%Y-%m-%dT%H:%M:%S.%f'
FORMAT_ALT = '%Y-%m-%dT%H:%M:%S'
custom_coins = {'DENA': 0.002567,  # not on coingecko as of 6.4.2021
                'USDC': 0.00049}


def ingest_nft_event(jjj, i, asset_type, coingecko):

    if 'token_id' in jjj['asset_events'][i][asset_type]:
        asset_id = jjj['asset_events'][i][asset_type]['token_id']
        # total_tokens['count'] += 1
    else:
        asset_id = '|'.join([a['token_id'] for a in jjj['asset_events'][i][asset_type]['assets']])
        # total_tokens['count'] += len(jjj['asset_events'][i][asset_type]['assets'])

    try:
        when = datetime.strptime(jjj['asset_events'][i]['created_date'], FORMAT_MAIN)
    except:
        when = datetime.strptime(jjj['asset_events'][i]['created_date'], FORMAT_ALT)
    event_type = jjj['asset_events'][i]['event_type']
    bid_amount = int(jjj['asset_events'][i]['bid_amount']) / eth if isinstance(
        jjj['asset_events'][i]['bid_amount'], str) else None
    list_price = int(jjj['asset_events'][i]['ending_price']) / eth if isinstance(
        jjj['asset_events'][i]['ending_price'], str) else None
    total_price = int(jjj['asset_events'][i]['total_price']) / eth if isinstance(
        jjj['asset_events'][i]['total_price'], str) else None
    ending_price = int(jjj['asset_events'][i]['ending_price']) / eth if isinstance(
        jjj['asset_events'][i]['ending_price'], str) else None
    starting_price = int(jjj['asset_events'][i]['starting_price']) / eth if isinstance(
        jjj['asset_events'][i]['starting_price'], str) else None
    owner = ''
    if 'owner' in jjj['asset_events'][i][asset_type] and 'user' in jjj['asset_events'][i][asset_type]['owner']:
        user = jjj['asset_events'][i][asset_type]['owner']['user']
        if user:
            owner = jjj['asset_events'][i][asset_type]['owner']['user']['username']

    # the following code checks if the token was listed in other coins (not ETH) and tries to convert the price
    # (the conversion is not accurate because it is done using CoinGecko data as of today
    eth_conversion_ratio = 1.0
    try:
        eth_conversion_ratio = float(jjj['asset_events'][i]['payment_token']['eth_price'])
    except:
        try:
            currency = jjj['asset_events'][i]['payment_token']['symbol']
            coingecko_id = None
            if currency not in custom_coins:
                for coin in coingecko:
                    if coin['symbol'].upper() == currency or coin['symbol'].upper() == currency[:-1]:
                        coingecko_id = coin['id']
                        break
                if coingecko_id:
                    url = f'https://api.coingecko.com/api/v3/simple/price?ids=%s&vs_currencies=ETH' % coingecko_id
                    eth_conversion_ratio = requests.get(url).json()[coingecko_id]['eth']
                    custom_coins[currency] = eth_conversion_ratio
                else:
                    print("Cannot convert %s to ETH. Skip this event" % currency)
                    return
            else:
                eth_conversion_ratio = custom_coins[currency]
        except:
            a = 1
    price = bid_amount if bid_amount else list_price if list_price else total_price if total_price else ending_price if ending_price else starting_price
    if price:
        if eth_conversion_ratio != 1.0:
            # if custom currency used, its decimals should be used and not the default ones
            price = price * eth / math.pow(10, int(jjj['asset_events'][i]['payment_token']['decimals'])) * eth_conversion_ratio
        else:
            price = price * eth_conversion_ratio

    if event_type == 'created' and not price:  # don't register create listing events with no price
        return

    try:
        OpenseaEvent.create(
            event_id=jjj['asset_events'][i]['id'],
            event_type=event_type,
            token_id=asset_id,
            price=price if price else 0,
            url=jjj['asset_events'][i][asset_type]['permalink'],
            when=when,
            num_sales=jjj['asset_events'][i][asset_type]['num_sales'],
            collection=jjj['asset_events'][i][asset_type]['collection']['slug'],
            owner=owner
        )
    except IntegrityError:
        print("Event %s already added" % jjj['asset_events'][i]['id'])

    return True


def opensea_data(argv):

    print("Getting list of coins from Coingecko...")
    coins = json.loads(requests.get('https://api.coingecko.com/api/v3/coins/list').text)

    with db:
        db.create_tables([OpenseaEvent])

    c1 = 0
    for g in list(glob.glob(argv[0])):
        print('ingesting data from %s' % g)
        f = open(g, 'r')
        try:
            j = json.load(f)
        except:
            continue
        if 'asset_events' not in j or j['asset_events'] == {}:
            continue
        for event in range(0, len(j['asset_events'])):
            if j['asset_events'][event]['asset']:
                c1 = c1 + 1
                ingest_nft_event(j, event, 'asset', coins)
            # elif 'asset_bundle' in j['asset_events'][event]:
            #     print("todo")
                # create_nft_event(j, event, 'asset_bundle', coins)
        f.close()


if __name__ == '__main__':
    opensea_data(sys.argv[1:])

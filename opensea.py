# Usage python3 opensea.py /Users/o.buistov/projects/crypto/opensea-analytics/new/*.json cryptopunk.csv
import json
import glob
from collections import defaultdict
from datetime import datetime
import csv
import sys
import requests
import numpy as np
import math


eth = 1e18
nft_activity = defaultdict(list)
buckets = defaultdict(lambda: defaultdict(int))  # funky eh?
FORMAT_MAIN = '%Y-%m-%dT%H:%M:%S.%f'
FORMAT_ALT = '%Y-%m-%dT%H:%M:%S'
custom_coins = {'DENA': 0.002567,  # not on coingecko as of 6.4.2021
                'USDC': 0.00049}


def create_nft_event(jjj, i, asset_type, coingecko):

    if 'token_id' in jjj['asset_events'][i][asset_type]:
        asset_id = jjj['asset_events'][i][asset_type]['token_id']
        # total_tokens['count'] += 1
    else:
        asset_id = '|'.join([a['token_id'] for a in jjj['asset_events'][i][asset_type]['assets']])
        # total_tokens['count'] += len(jjj['asset_events'][i][asset_type]['assets'])

    existing_ids = [x['id'] for x in nft_activity[asset_id]]

    if jjj['asset_events'][i]['id'] not in existing_ids:

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
                price = price * eth / math.pow(10, int(jjj['asset_events'][i]['payment_token']['decimals'])) * eth_conversion_ratio
            else:
                price = price * eth_conversion_ratio

        if event_type == 'created' and not price:
            return
        nft_activity[asset_id].append({
            'id': jjj['asset_events'][i]['id'],
            'when': when,
            'token_id': asset_id,
            'event': event_type,
            'price': price if price else 0,
            # 'url': jjj['asset_events'][i][asset_type]['permalink'],
        })
        nft_activity[asset_id].sort(key=lambda d: d['when'])

        when = when.strftime('%b %d, %Y')
        if event_type == 'successful':
            buckets[when]['sales'] += 1
            if price:
                buckets[when]['volume'] += price
            else:
                buckets[when]['volume'] += 0
    else:
        print("Duplicate event %s" % jjj['asset_events'][i]['id'])

    return True


def opensea_data(argv):

    print("Getting list of coins from Coingecko...")
    coins = json.loads(requests.get('https://api.coingecko.com/api/v3/coins/list').text)

    c1 = 0
    for g in list(glob.glob(argv[0])):
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
                create_nft_event(j, event, 'asset', coins)
            # elif 'asset_bundle' in j['asset_events'][event]:
            #     print("todo")
                # create_nft_event(j, event, 'asset_bundle', coins)
        f.close()

    currently_on_sale = 0
    listed_prices = []
    for events in nft_activity.values():
        last_list_event = -1
        last_sold_event = -1
        last_cancelled_event = -1
        last_transferred_event = -1
        list_price = 0.0
        for e in events:
            if e['event'] == 'created':
                last_list_event = events.index(e)
                list_price = e['price']
            if e['event'] == 'cancelled':
                last_cancelled_event = events.index(e)
            if e['event'] == 'successful':
                last_sold_event = events.index(e)
            if e['event'] == 'transfer':
                last_transferred_event = events.index(e)

        if last_list_event > last_sold_event and last_list_event > last_cancelled_event and last_list_event > last_transferred_event:
            currently_on_sale += 1
            listed_prices.append(list_price)

    primary_sales = 0
    single_sale = 0
    single_sale_price = []
    secondary_sales = 0
    secondary_sale_price = []
    for events in nft_activity.values():
        num_sales = len([e for e in events if e['event'] == 'successful' and e['price'] > 0])
        if num_sales == 0:
            primary_sales += 1  # no data here. need to fetch events
        elif num_sales == 1:
            single_sale += 1
            sngls = [e['price'] for e in events if e['event'] == 'successful' and e['price'] > 0]
            if len(sngls) > 0:
                single_sale_price.append(sngls[0])
        else:
            secondary = [e['price'] for e in events if e['event'] == 'successful' and e['price'] > 0]
            secondary_sales += 1
            secondary_sale_price.extend(secondary[1:])

    print("Total tokens in dataset: %d" % len(nft_activity))
    print("Total events in dataset: %d" % c1)
    # never_traded = sorted(set(range(1, TOTAL_SUPPLY)).difference(set([int(x) for x in nft_activity.keys()])))
    # print("%d tokens never traded" % len(never_traded))
    print("%d tokens currently on sale" % currently_on_sale)
    print("on-sale min price: %f" % np.min(listed_prices))
    # print("on-sale avg price: %f" % np.average(listed_prices))
    print("on-sale 50 percentile: %f" % np.percentile(listed_prices, 50))
    print("on-sale 95 percentile: %f" % np.percentile(listed_prices, 90))
    print("on-sale 99 percentile: %f" % np.percentile(listed_prices, 99))
    # print("on-sale max: %f" % np.max(listed_prices))

    print("primary market: %d items" % primary_sales)
    print("single sale: %d items" % single_sale)
    print("single sale min price: %f" % np.min(single_sale_price))
    # print("single sale avg price: %f" % np.average(single_sale_price))
    print("single sale 50 percentile: %f" % np.percentile(single_sale_price, 50))
    print("single sale 95 percentile: %f" % np.percentile(single_sale_price, 90))
    print("single sale 99 percentile: %f" % np.percentile(single_sale_price, 99))
    print("single sale max: %f" % np.max(single_sale_price))

    print("secondary market: %d items" % secondary_sales)
    print("secondary market min price: %f" % np.min(secondary_sale_price))
    # print("secondary market avg price: %f" % np.average(secondary_sale_price))
    print("secondary market 50 percentile: %f" % np.percentile(secondary_sale_price, 50))
    print("secondary market 95 percentile: %f" % np.percentile(secondary_sale_price, 90))
    print("secondary market 99 percentile: %f" % np.percentile(secondary_sale_price, 99))
    print("secondary market max: %f" % np.max(secondary_sale_price))

    this_year_data = {key: value for (key, value) in buckets.items()
                      if datetime.strptime(key, '%b %d, %Y').strftime('%Y') == '2021'}
    sorted_data = dict(sorted(this_year_data.items(), key=lambda x: datetime.strptime(x[0], '%b %d, %Y')))
    with open(argv[1], 'w', newline='') as csvfile:
        fieldnames = ['Day', 'Sales', 'Volume']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for k, v in sorted_data.items():
            writer.writerow(
                {
                    fieldnames[0]: k,
                    fieldnames[1]: v['sales'],
                    fieldnames[2]: str("{:.2f}".format(v['volume']))  # .replace('.', ',')
                })


if __name__ == '__main__':
    opensea_data(sys.argv[1:])

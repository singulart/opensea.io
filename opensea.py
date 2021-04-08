# Usage python3 opensea.py /Users/o.buistov/projects/crypto/opensea-analytics/new/*.json cryptopunk.csv
import csv
import sys
from collections import defaultdict
from datetime import datetime

import numpy as np
from playhouse.shortcuts import model_to_dict

from db_models import *

nft_activity = defaultdict(list)
buckets = defaultdict(lambda: defaultdict(int))  # funky eh?


def opensea_data(argv):

    for record in OpenseaEvent.select().order_by(OpenseaEvent.token_id, OpenseaEvent.when.asc()):
        nft_activity[record.token_id].append(model_to_dict(record))
        if record.event_type == 'successful':
            when = record.when.strftime('%b %d, %Y')
            buckets[when]['sales'] += 1
            if record.price:
                buckets[when]['volume'] += record.price
            else:
                buckets[when]['volume'] += 0

    currently_on_sale = 0
    listed_prices = []
    for events in nft_activity.values():
        last_list_event = -1
        last_sold_event = -1
        last_cancelled_event = -1
        last_transferred_event = -1
        list_price = 0.0
        for e in events:
            if e['event_type'] == 'created':
                last_list_event = events.index(e)
                list_price = e['price']
            if e['event_type'] == 'cancelled':
                last_cancelled_event = events.index(e)
            if e['event_type'] == 'successful':
                last_sold_event = events.index(e)
            if e['event_type'] == 'transfer':
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
        num_sales = len([e for e in events if e['event_type'] == 'successful' and e['price'] > 0])
        if num_sales == 0:
            primary_sales += 1  # no data here. need to fetch events
        elif num_sales == 1:
            single_sale += 1
            sngls = [e['price'] for e in events if e['event_type'] == 'successful' and e['price'] > 0]
            if len(sngls) > 0:
                single_sale_price.append(sngls[0])
        else:
            secondary = [e['price'] for e in events if e['event_type'] == 'successful' and e['price'] > 0]
            secondary_sales += 1
            secondary_sale_price.extend(secondary[1:])

    print("Total tokens in dataset: %d" % len(nft_activity))
    # print("Total events in dataset: %d" % c1)
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
    opensea_data(sys.argv)

import sys
from collections import defaultdict
from datetime import datetime
from random_dict import RandomDict
from db_models import *
from json import encoder
import json
from decimal import Decimal as D

# only report seller losses greater than this percentage
loss_threshold = 60

nft_activity = RandomDict(defaultdict(list))
loss = defaultdict(list)
encoder.FLOAT_REPR = lambda o: format(o, '.2f')


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, D):
            return float(obj)
        return json.JSONEncoder.default(self, obj)


def opensea_data(argv):

    query = OpenseaEvent.select()\
                .where(OpenseaEvent.num_sales > 1, OpenseaEvent.event_type == 'successful')\
                .order_by(OpenseaEvent.collection, OpenseaEvent.token_id, OpenseaEvent.when.asc())
    total = query.count()
    print('Total events %d' % total)

    begin = datetime.now()
    for rec in query.execute():
        combo_key = ''.join([rec.collection, rec.token_id])
        if combo_key not in nft_activity:
            nft_activity[combo_key] = []
        nft_activity[combo_key].append(rec)
    print("query took %d s. fetched %d records" % ((datetime.now() - begin).microseconds / 1000, len(nft_activity.values)))

    for events in nft_activity.values:
        last_sold_price = 0.0
        asset_events = events[1]
        sale_stats = defaultdict(lambda: defaultdict(int))  # funky eh?
        for e in range(0, len(asset_events)):
            if asset_events[e].event_type == 'successful':
                if asset_events[e].seller and asset_events[e].seller != '':
                    sale_stats[asset_events[e].seller]['sold'] += asset_events[e].price
                if asset_events[e].winner and asset_events[e].winner != '':
                    sale_stats[asset_events[e].winner]['bought'] += asset_events[e].price

                # if 0 < asset_events[e].price < last_sold_price:
                #     loss_percentage = 100 - asset_events[e].price / last_sold_price * 100
                #     break
                # last_sold_price = events[1][e].price
        for k, v in sale_stats.items():
            if 'sold' in v.keys() and 'bought' in v.keys() and v['sold'] < v['bought']:
                loss_percentage = 100 - v['sold'] / v['bought'] * 100
                if loss_percentage >= loss_threshold:
                    loss[asset_events[e].collection].append(
                        {'url': asset_events[e].url, 'loss': str("{:.2f}".format(loss_percentage))})

    with open('loss.json', 'w') as output:
        output.write(json.dumps(loss))


if __name__ == '__main__':
    opensea_data(sys.argv)

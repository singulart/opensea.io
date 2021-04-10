import sys
from collections import defaultdict
from datetime import datetime
from random_dict import RandomDict
from db_models import *
from json import encoder
import json
from decimal import Decimal as D

# only report seller losses greater than this percentage
loss_threshold = 20

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
        for e in range(0, len(events[1])):
            if events[1][e].event_type == 'successful':
                if 0 < events[1][e].price < last_sold_price:
                    loss_percentage = 100 - events[1][e].price / last_sold_price * 100
                    if loss_percentage >= loss_threshold:
                        loss[events[1][e].collection].append(
                            {'url': events[1][e].url, 'loss': str("{:.2f}".format(loss_percentage))})
                    break
                last_sold_price = events[1][e].price
    with open('loss.json', 'w') as output:
        output.write(json.dumps(loss))


if __name__ == '__main__':
    opensea_data(sys.argv)

from peewee import *

db = SqliteDatabase('opensea.db')


class OpenseaEvent(Model):
    id = AutoField()  # Auto-incrementing primary key.
    event_id = IntegerField(unique=True)
    event_type = CharField()
    token_id = CharField(index=True)
    price = FloatField(null=True, index=True)
    when = DateTimeField()
    url = CharField()
    num_sales = CharField()
    collection = CharField(index=True)
    owner = CharField

    class Meta:
        database = db
        table_name = 'os_events'

from peewee import *

db = SqliteDatabase('people.db')
pg_db = PostgresqlDatabase(
    'den4ncj6b3nja4',
    user='pxyerzwuholdqp',
    password='e45b07a7306a8868f1ebc5bf3a63d46c8a8416e602f1749c4af90f44ee96140e',
    host='ec2-34-204-22-76.compute-1.amazonaws.com', port=5432
)

pg_db.connect()

class BaseModel(Model):

    class Meta:
        database = pg_db


class Cities(Model):
    city_id = AutoField(primary_key=True)
    city_name = CharField(max_length=255)
    city_state = CharField(max_length=255, blank=True, null=True)
    city_country = CharField(max_length=255)
    metropolitan = ForeignKeyField(MetropolitanArea, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'cities'

class Venues(Model):
    venue_id = AutoField(primary_key=True)
    city = ForeignKeyField(Cities, blank=True, null=True)
    venue_name = CharField(max_length=255)
    venue_address = CharField(max_length=255)

    class Meta:
        managed = False
        db_table = 'venues'

class Events(Model):
    event_id = AutoField(primary_key=True)
    venue = ForeignKeyField(Venues, blank=True)
    event_name = CharField(max_length=255)
    event_start_at = TimeField(blank=True, null=True)
    event_end_at = TimeField(blank=True, null=True)
    tickets_link = CharField(max_length=255, blank=True, null=True)
    event_date = DateField(blank=True, null=True)
    event_type = CharField(max_length=25)
    created_on = DateTimeField()

    class Meta:
        database = pg_db

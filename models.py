from peewee import *

pg_db = PostgresqlDatabase(
    'den4ncj6b3nja4',
    user='pxyerzwuholdqp',
    password='e45b07a7306a8868f1ebc5bf3a63d46c8a8416e602f1749c4af90f44ee96140e',
    host='ec2-34-204-22-76.compute-1.amazonaws.com', port=5432
)

class BaseModel(Model):

    class Meta:
        database = pg_db

class Artists(Model):
    artist_id = AutoField(primary_key=True)
    artist_name = CharField(max_length=155)

    class Meta:
        managed = False
        database = pg_db

class MetropolitanArea(Model):
    metropolitan_id = AutoField(primary_key=True)
    metropolitan_name = CharField(max_length=50)

    class Meta:
        database = pg_db
        db_table = 'metropolitan_area'

class Cities(Model):
    city_id = AutoField(primary_key=True)
    city_name = CharField(max_length=255)
    city_state = CharField(max_length=255, null=True)
    city_country = CharField(max_length=255)
    metropolitan = ForeignKeyField(MetropolitanArea, null=True)

    class Meta:
        managed = False
        database = pg_db

class Venues(Model):
    venue_id = PrimaryKeyField(primary_key=True)
    city = ForeignKeyField(Cities, backref='cities', null=True)
    venue_name = CharField(max_length=255)
    venue_address = CharField(max_length=255)

    class Meta:
        managed = False
        database = pg_db

class Events(Model):
    event_id = AutoField(primary_key=True)
    venue = ForeignKeyField(Venues)
    event_name = CharField(max_length=255)
    event_start_at = TimeField(null=True)
    event_end_at = TimeField(null=True)
    tickets_link = CharField(max_length=255, null=True)
    event_date = DateField(null=True)
    event_type = CharField(max_length=25)
    created_on = DateTimeField()

    class Meta:
        database = pg_db

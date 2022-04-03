from peewee import *
from dotenv import load_dotenv
from os import environ, path

basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, ".env"))
pg_db = PostgresqlDatabase(
    environ("DB_NAME"),
    user=environ("DB_USER"),
    password=environ("DB_PW"),
    host=environ("DB_HOST"),
    port=environ("DB_PORT"),
)


class BaseModel(Model):
    class Meta:
        database = pg_db


class Artists(Model):
    id = AutoField(primary_key=True)
    name = CharField(max_length=155)

    class Meta:
        managed = False
        database = pg_db


class Genres(Model):
    id = AutoField(primary_key=True)
    name = CharField(max_length=50)

    class Meta:
        database = pg_db


class ArtistGenre(Model):
    id = AutoField(primary_key=True)
    artist_id = ForeignKeyField(Artists, null=False)
    genre_id = ForeignKeyField(Genres, null=False)

    class Meta:
        database = pg_db
        db_table = "artist_genre"


class MetropolitanArea(Model):
    id = AutoField(primary_key=True)
    name = CharField(max_length=50)

    class Meta:
        database = pg_db
        db_table = "metropolitan_area"


class Cities(Model):
    id = AutoField(primary_key=True)
    name = CharField(max_length=255)
    state = CharField(max_length=255, null=True)
    country = CharField(max_length=255)
    metropolitan_id = ForeignKeyField(MetropolitanArea, null=True)

    class Meta:
        managed = False
        database = pg_db


class Venues(Model):
    id = PrimaryKeyField(primary_key=True)
    city_id = ForeignKeyField(Cities, null=True)
    name = CharField(max_length=255)
    address = CharField(max_length=255)

    class Meta:
        managed = False
        database = pg_db


class Events(Model):
    id = AutoField(primary_key=True)
    venue_id = ForeignKeyField(Venues, null=False)
    name = CharField(max_length=255)
    start_at = TimeField(null=True)
    end_at = TimeField(null=True)
    is_active = BooleanField(null=False)
    tickets_link = CharField(max_length=255, null=True)
    date = DateField(null=True)
    type = CharField(max_length=25)
    created_on = DateTimeField()

    class Meta:
        database = pg_db


class Event_Artist(Model):
    id = AutoField(primary_key=True)
    event_id = ForeignKeyField(Events, null=False)
    artist_id = ForeignKeyField(Artists, null=False)
    headliner = BooleanField()

    def __repr__(self):
        return "{0}".format(self.event_id)

    class Meta:
        database = pg_db

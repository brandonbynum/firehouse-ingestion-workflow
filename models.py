from typing import List
import peewee
from dotenv import load_dotenv
from os import environ, path
import logging

basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, ".env"))

pg_db = peewee.PostgresqlDatabase(
    environ.get("DB_NAME"),
    user=environ.get("DB_USER"),
    password=environ.get("DB_PW"),
    host=environ.get("DB_HOST"),
    port=environ.get("DB_PORT"),
)
class BaseModel(peewee.Model):
    class Meta:
        database = pg_db

class Artists(peewee.Model):
    name = peewee.CharField(max_length=155)

    # def __repr__(self):
    #     return f"<Artist {self.id}, {self.name} />"

    @classmethod
    def get(cls, id: int = None, name: str = None):
        if id and name:
            return cls.select().where(cls.id == id and cls.name == name)
        elif id and not name:
            return cls.select().where(cls.id == id)
        elif name and not id:
            return cls.select().where(cls.name == name)
        return cls.select()
    
    @classmethod
    def create_many(cls, artist_names: List):
        try:
            with pg_db.atomic():
                cls.insert_many(artist_names).execute()
            logging.info(f"\t{len(artist_names)} Artist created")
        except:
            logging.info(f"\t Error creating Artists")

    class Meta:
        managed = False
        database = pg_db

class Genres(peewee.Model):
    name = peewee.CharField(max_length=50)

    def get(id: int = None, name: str = None):
        if id and name:
            return Genres.select().where(Genres.id == id and Genres.name == name)
        elif id and not name:
            return Genres.select().where(Genres.id == id)
        elif name and not id:
            return Genres.select().where(Genres.name == name)
        return Genres.select()

    class Meta:
        database = pg_db


class ArtistGenre(peewee.Model):
    artist_id = peewee.ForeignKeyField(Artists, backref="artists", null=False)
    genre_id = peewee.ForeignKeyField(Genres, backref="genres", null=False)

    @classmethod
    def get(cls):
        return (
            cls.select(ArtistGenre, Artists.name.alias("artist_name"), Genres.name.alias("genre_name"))
            .join(Genres, on=(ArtistGenre.genre_id == Genres.id))
            .switch(ArtistGenre)
            .join(Artists, on=(ArtistGenre.artist_id == Artists.id))
        )

    @classmethod
    def get_by_artist(cls, artist_name: str):
        return cls.select().where(cls.artist_name == artist_name)

    @classmethod
    def get_by_genre(cls, genre_name: str):
        return cls.select().where(cls.genre_name == genre_name)

    @classmethod
    def create_many(cls, models: List):
        try:
            with pg_db.atomic():
                cls.insert_many(models).execute()
            logging.info(f"\t{len(models)} ArtistGenre models created")
        except:
            logging.error(f"\t Error creating ArtistGenre models")
            
    class Meta:
        database = pg_db
        db_table = "artist_genres"


class MetropolitanArea(peewee.Model):
    name = peewee.CharField(max_length=50)

    class Meta:
        database = pg_db
        db_table = "metropolitan_area"
    


class Cities(peewee.Model):
    name = peewee.CharField(max_length=255)
    state = peewee.CharField(max_length=255, null=True)
    country = peewee.CharField(max_length=255)
    metropolitan_id = peewee.ForeignKeyField(MetropolitanArea, null=True)

    @classmethod
    def get(cls):
        return cls.select(cls, MetropolitanArea.name.alias("metropolitan_name")).join(
            MetropolitanArea, on=(MetropolitanArea.id == cls.metropolitan_id)
        )

    class Meta:
        managed = False
        database = pg_db

class Venues(peewee.Model):
    city_id = peewee.ForeignKeyField(Cities, null=True)
    name = peewee.CharField(max_length=255)
    address = peewee.CharField(max_length=255)
    
    @classmethod
    def get_as_dict(cls, expr):
        return cls.select().where(expr).dicts().get()

    class Meta:
        managed = False
        database = pg_db

class Events(peewee.Model):
    created_on = peewee.DateTimeField()
    date = peewee.DateField(null=True)
    end_at = peewee.TimeField(null=True)
    is_active = peewee.BooleanField(null=False)
    name = peewee.CharField(max_length=255)
    start_at = peewee.TimeField(null=True)
    tickets_link = peewee.CharField(max_length=255, null=True)
    type = peewee.CharField(max_length=25)
    venue_id = peewee.ForeignKeyField(Venues, null=False)

    def __repr__(self):
        return f"<Event {self.name}, {self.type}, {self.date}>"

    @classmethod
    def get(cls, id: int = None, name: str = None):
        if id and name:
            return cls.select().where(cls.id == id and cls.name == name)
        elif id and not name:
            return cls.select().where(cls.id == id)
        elif name and not id:
            return cls.select().where(cls.name == name)
        return cls.select()
    
    class Meta:
        database = pg_db

class Event_Artist(peewee.Model):
    event_id = peewee.ForeignKeyField(Events, null=False)
    artist_id = peewee.ForeignKeyField(Artists, null=False)
    headliner = peewee.BooleanField()

    def __repr__(self):
        return "{0}".format(self.event_id)
    class Meta:
        database = pg_db

from models import Event_Artist, Events, pg_db

def insert_events(event_details: dict):
    event_artist_models = []
    
    #TODO: Exception handling so one failure doesn't hold up reest of inserts
    for key in event_details.keys():
        event_to_import = event_details[key]
        
        with pg_db.atomic():
            event_insert = Events.get_or_create(
                date=event_to_import["date"],
                venue_id=event_to_import["venue_id"],
                name=event_to_import["name"],
                tickets_link=event_to_import["ticket_url"],
                defaults={
                    "type": "Concert",
                    "is_active": True,
                    "start_at": event_to_import["start_at"],
                    "end_at": None
                }
            )
        print(event_details[key])
            
        event_artist_models.append({
            "artist_id": key,
            "event_id": event_insert[0].id,
        })
        
    # TODO (Performance): Add Unique Constraint to event_artist table and insert using 'insert_many'
    for model in event_artist_models:
        with pg_db.atomic():
            event_artist_insert = Event_Artist.get_or_create(
                artist_id=model["artist_id"],
                event_id=model["event_id"],
                defaults={"headliner": True}
            )
            print("\n", model, event_artist_insert[1])

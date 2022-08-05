from models import Event_Artist, Events, pg_db

def insert_events(event_details: dict):
    event_artist_models = []
    
    for key in event_details.keys():
        event_to_import = event_details[key]
        try:
            with pg_db.atomic():
                event_insert = Events.get_or_create(
                    date=event_to_import["date"],
                    end_date=event_to_import["end_date"],
                    venue_id=event_to_import["venue_id"],
                    name=event_to_import["name"],
                    tickets_link=event_to_import["ticket_url"],
                    defaults={
                        "type": event_to_import["type"],
                        "start_at": event_to_import["start_at"],
                        "end_at": None
                    }
                )
            insert_status = "successful insert" if event_insert[1] else "record already exists"
            print(f"\n{event_details[key]} --> {insert_status}")
                
            event_artist_models.append({
                "artist_id": key,
                "event_id": event_insert[0].id,
            })
        except Exception as e:
            print(f"Failed to insert the following event\n\t{event_details[key]}: {e}")
        
    # TODO (Performance): Add Unique Constraint to event_artist table and insert using 'insert_many'
    for model in event_artist_models:
        with pg_db.atomic():
            event_artist_insert = Event_Artist.get_or_create(
                artist_id=model["artist_id"],
                event_id=model["event_id"],
                defaults={"headliner": True}
            )
        insert_status = "successful insert" if event_artist_insert[1] else "record already exists"
        print(f"\n{model} --> {insert_status}")

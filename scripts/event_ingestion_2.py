import asyncio
from copy import error
from datetime import datetime
from bs4 import BeautifulSoup
import os
import requests
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from models import *
from utilities.data_scraper import data_scraper
from utilities.truncate_and_write import truncate_and_write
from utilities.get_file_text import get_file_text
from utilities.insert_events import insert_events

basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, ".env"))

async def main():
    url = environ.get("EVENT_INGESTION_URL")

    try:
        html = requests.get(url).content
    except:
        print(f"\tFailed to retrieve content")
        
    soup = BeautifulSoup(html, features="html.parser")
    all_a_elements = soup.find_all("a", class_="small")
    
    if len(all_a_elements) == 0:
        print("No events to import...")
        exit()
        
    # Transform and validate events to import
    print("Number of Events: %s" % len(all_a_elements))
    artists_dict = {artist["name"]: artist["id"] for artist in Artists.get().dicts()}
    cities_dict = {city["name"]: city["id"] for city in Cities.get().dicts()}
    
    events_to_import = {}
    for element in all_a_elements:
        print()
        event_html = data_scraper('get', element['href']).content
        event_detail_soup = BeautifulSoup(event_html, features="html.parser")
        
        # ARTIST NAME value extraciton
        try:
            # TODO: Possible Case --- 'Rufus Du Sol, Claptone, Green Velvet, Gordo'
            p_tags = event_detail_soup.find_all("p")
            for tag in p_tags:
                strong_tag = tag.find("strong")
                if "Headliner" in strong_tag.text:
                    artist = tag
                    break
            artist.strong.clear()
            artist = artist.text
            print(f"Artist: {artist}")
        except:
            print('Error extracting event artist.')
            
        # CITY NAME value extraction
        try: 
            location_list = event_detail_soup.find("span", "location").text.split(",")
            city = location_list[0].strip()
            print(f"City: {city}")
        except:
            print('Error extracting event location value.')
        
        # Check if event is relevant
        if artist in artists_dict.keys() and city in cities_dict.keys():    
            # Extract event title from event element
            try:
                title = event_detail_soup.find(id="event-title").text
                print(f"Title: {title}")
            except:
                print('Error extracting event title.')
            
            # DATE value extraction
            try:
                # TODO: Possible Case --- 'June 17-19 2022'
                date_element = event_detail_soup.find(id="date")
                for span in date_element.find_all("span"): span.clear()
                date_text = ''.join(date_element.text.split('at')[0].strip().split(','))
                date = datetime.strptime(date_text, "%B %d %Y")
                print(f"Date: {date}")
            except:
                print('Error extracting or transforming event date text.')
                continue
            
            # START AT value extraction
            try:
                extracted_start_at = date_element.text.split('at')[1].strip()
                start_at = time.strftime("%H:%M", time.strptime(extracted_start_at, "%I:%M %p"))
                print(f"Start at: {start_at}")
                
            except:
                print('Error extracting start at time from event date value.')
                
            # VENUE NAME value extraction
            try:
                venue = event_detail_soup.find("span", "venue").text
                print(f"Venue: {venue}")
            except:
                print('Error extracting event venue value.')
                
            # TICKET LINK value extraction
            try:
                ticket_link = event_detail_soup.find("a", title="Buy Tickets for")['href']
                print(f"Tickets: {ticket_link}")
            except:
                print('Error extracting event ticket link value.')
            
            
            venue_id = Venues.get_or_create(
                name=venue,
                defaults={"address": "N/A", "city_id": cities_dict[city]}
            )[0].id
            
            events_to_import[artists_dict[artist]] = {
                "date": date,
                "city": cities_dict[city],
                "name": title,
                "start_at": start_at,
                "ticket_url": ticket_link,
                "venue_id": venue_id
            }
    insert_events(events_to_import)
    
if __name__ == "__main__":
    asyncio.run(main())

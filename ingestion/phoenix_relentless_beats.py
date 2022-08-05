import asyncio
from datetime import datetime
from bs4 import BeautifulSoup
import os
import requests
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from models import *
from utilities.request_html import request_html
from utilities.insert_events import insert_events
from utilities.request_html_as_soup import request_html_as_soup

basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, ".env"))
    
def extract_artist_names(soup, relevant_artists):
    """Locate headliner and support act names

    Args:
        soup (BeautifulSoup): object representing HTML of event detail page
        relevant_artists (dict): _description_

    Returns:
        dict: 
            artist_id (int): artist id in artists table, 
            headliner (bool): if artist's billing is headliner
    """
    event_artists = {}
    try:
        p_tags = soup.find_all("p")
        billings = ["headliner", "support"]
        billing_finds = 0

        for p_tag in p_tags:
            if billing_finds == 2:
                break
                            
            if any([item in p_tag.get_text().lower() for item in billings]):
                billing_finds += 1
                try:
                    billing = [item for item in billings if item in p_tag.get_text().lower()][0]
                    p_tag.strong.clear()
                    artists_text = p_tag.get_text()
                    
                    if ',' in artists_text:
                        for artist_text in artists_text.split(','):
                            # TODO: Create reusable func
                            artist_stripped = artist_text.strip()
                            if artist_stripped in relevant_artists.keys():
                                print(f"{artist_stripped} <--")
                                artist_id = relevant_artists[artist_stripped]
                                event_artists[artist_id] = {"headliner": True if billing == "headliner" else False}
                            else:
                                print(artist_stripped)
                    elif artists_text != '':
                        # TODO: Create reusable func
                        artist_stripped = artists_text.strip()
                        if artist_stripped in relevant_artists.keys():
                            print(f"{artist_stripped} <--")
                            artist_id = relevant_artists[artist_stripped]
                            event_artists[artist_id] = {"headliner": True if billing == "headliner" else False}
                        else:
                            print(artist_stripped)
                except Exception as e:
                    print(f"Failed to extract '{biilling}': {e}")                  
    except:
        print('Error extracting event artist.')
    return event_artists

def extract_city_name(soup):
    """Locate city name

    Args:
        soup (BeautifulSoup): object representing HTML of event detail page

    Returns:
        string: Name of city where event is occurring
    """
    city = None
    try: 
        location_list = soup.find("span", "location").text.split(",")
        city = location_list[0].strip()
        print(city)
    except:
        print('Error extracting event location value.')
    return city

def extract_event_name(soup):
    """Locate name of event

    Args:
        soup (BeautifulSoup): object representing HTML of event detail page

    Returns:
        string: Name of the event
    """
    try:
        name = soup.find(id="event-title").get_text()
        return name
    except:
        print('Error extracting event name.')
        
def extract_date(soup):
    """Locate date and starting time of event

    Args:
        soup (BeautifulSoup): object representing HTML of event detail page

    Returns:
        dict: 
            date: datetime,
            start_at: string
    """
    extracted = {"date": None, "start_at": None, "end_date": None}
    
    try:
        date_element = soup.find(id="date")
        for span in date_element.find_all("span"): span.clear()

        date_text = date_element.get_text()
        # CASE (multi-day event / festival)
        # Converts format from 'October 7-9, 2022' to 'YYYY-MM-DD 00:00:00'
        if "-" in date_text:
            # -> ['October', '7-9,', '2022']
            date_split_hyphen = date_text.split(" ")
            month = date_split_hyphen[0]
            year = date_split_hyphen[2]
            # -> 7-9 or None
            day_range = next((days.split(',')[0] for days in date_split_hyphen if '-' in days), None).split("-")
            
            for index, day in enumerate(day_range):
                date_string = ' '.join([month, str(day), year])                
                date = datetime.strptime(date_string, "%B %d %Y")                
                if index == 0:
                    extracted["date"] = date
                    
                if index == len(day_range) - 1:
                    extracted["end_date"] = date
        else:
            # CASE (single day event)
            # Converts format from 'July 3, 2022 at 9:00 PM' to 'YYYY-MM-DD 00:00:00'
            # -> ["July 3, 2022", "9:00 PM"] 
            # -> ["July 3", " 2022"] 
            # -> "July 3 2022" 
            # -> 2022-7-3 00:00:00
            date_split_at = date_element.text.split("at")
            date_text = ''.join(date_split_at[0].strip().split(','))
            date = datetime.strptime(date_text, "%B %d %Y")
            extracted["date"] = date
            
            try:
                extracted_start_at = date_split_at[1].strip()
                print(extracted_start_at)
                extracted["start_at"] = time.strftime("%H:%M", time.strptime(extracted_start_at, "%I:%M %p"))
                print(extracted["start_at"])
            except:
                print('Error extracting start at time from event date value.') 
    except:
        print(f'Error extracting or transforming event date text: {date_element}')
    
    return extracted          

def extract_type(soup):
    """Locate event type

    Args:
        soup (BeautifulSoup): object representing HTML of event detail page

    Returns:
        string: Type of event
    """
    p_tags = soup.find_all("p")
    
    for tag in p_tags:
        if "type" in tag.get_text().lower():
            tag.strong.clear()
            return tag.get_text()
    return "Concert"
                
def extract_venue(soup):
    """Locate name of venue

    Args:
        soup (BeautifulSoup): object representing HTML of event detail page

    Returns:
        string: name of venue
    """
    try:
        venue = soup.find("span", "venue").text
        return venue
    except:
        print('Error extracting event venue value.')
        
def extract_ticket_url(soup):
    """Locate ticket url

    Args:
        soup (BeautifulSoup): object representing HTML of event detail page

    Returns:
        string: url
    """
    try:
        ticket_link = soup.find("a", title="Buy Tickets for")['href']
        return ticket_link
    except:
        print('Error extracting event ticket link value.')


async def main():
    """Retrieves and extracts data from events on RelentlessBeats' event page which have artists
       or city that exist in the database.
    """
    url = "http://relentlessbeats.com/events"

    soup = request_html_as_soup(url)
    all_a_elements = soup.find_all("a", class_="small")
    
    if len(all_a_elements) == 0:
        print("No events to import...")
        exit()
        
    # Transform and validate events to import
    print("Number of Events: %s" % len(all_a_elements))
    
    try:
        artists_dict = {artist["name"]: artist["id"] for artist in Artists.get().dicts()}
        cities_dict = {city["name"]: city["id"] for city in Cities.get().dicts()}
        relevant_cities = cities_dict.keys()
    except Exception as e:
        print(e)
        exit()
    
    events_to_import = {}
    for element in all_a_elements:
        event_detail_soup = request_html_as_soup(element['href'])
        
        city_name = extract_city_name(event_detail_soup)
        if city_name not in relevant_cities:
            print()
            continue
        
        event_artists = extract_artist_names(event_detail_soup, artists_dict)
        if len(event_artists) == 0:
            print()
            continue
        
        extracted_date_time = extract_date(event_detail_soup)
        if extracted_date_time["date"] == None:
            print()
            continue

        event_name = extract_event_name(event_detail_soup)
        event_type = extract_type(event_detail_soup)
        venue = extract_venue(event_detail_soup)
        venue_id = Venues.get_or_create(
            name=venue,
            defaults={"address": "N/A", "city_id": cities_dict[city_name]}
        )[0].id
        ticket_link = extract_ticket_url(event_detail_soup)            
        
        for artist_id in event_artists.keys():
            event_details = {
                "date": extracted_date_time["date"],
                "end_date": extracted_date_time["end_date"],
                "city": cities_dict[city_name],
                "headliner": event_artists[artist_id]["headliner"],
                "name": event_name,
                "start_at": extracted_date_time["start_at"],
                "ticket_url": ticket_link,
                "type": event_type,
                "venue_id": venue_id,
            }
            events_to_import[artist_id] = event_details
            print(f"{event_details}\n")
        
    insert_events(events_to_import)
    
if __name__ == "__main__":
    asyncio.run(main())

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
                            artist_stripped = artist_text.strip()
                            print(artist_stripped)
                            if artist_stripped in relevant_artists.keys():
                                print("<--")
                                artist_id = relevant_artists[artist_stripped]
                                event_artists[artist_id] = {"headliner": True if billing == "headliner" else False}
                    elif artists_text != '':
                        artist_stripped = artists_text.strip()
                        print(artist_stripped)
                        if artist_stripped in relevant_artists.keys():
                            print("<--")
                            artist_id = relevant_artists[artist_stripped]
                            event_artists[artist_id] = {"headliner": True if billing == "headliner" else False}
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
    try:
        # TODO: Possible Case --- 'June 17-19 2022'
        date_element = soup.find(id="date")
        for span in date_element.find_all("span"): span.clear()
        
        # Converts format from 'July 3, 2022 at 9:00 PM' to 'YYYY-MM-DD 00:00:00'
        date_split_at = date_element.text.split('at')
        date_text = ''.join(date_split_at[0].strip().split(','))
        date = datetime.strptime(date_text, "%B %d %Y")
    except:
        print('Error extracting or transforming event date text.')
        
    try:
        extracted_start_at = date_split_at[1].strip()
        start_at = time.strftime("%H:%M", time.strptime(extracted_start_at, "%I:%M %p"))
    except:
        print('Error extracting start at time from event date value.') 
        
    return {'date': date, 'start_at': start_at}           
        
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
        event_html = request_html(element['href'])
        event_detail_soup = BeautifulSoup(event_html, features="html.parser")
        
        city_name = extract_city_name(event_detail_soup)
        event_artists = extract_artist_names(event_detail_soup, artists_dict)
        
        if len(event_artists) == 0:
            print("No artist found\n")
            continue
        
        if city_name not in relevant_cities:
            print()
            continue
          
        event_name = extract_event_name(event_detail_soup)
        date = extract_date(event_detail_soup)
        venue = extract_venue(event_detail_soup)
        venue_id = Venues.get_or_create(
            name=venue,
            defaults={"address": "N/A", "city_id": cities_dict[city_name]}
        )[0].id
        ticket_link = extract_ticket_url(event_detail_soup)            
        
        for artist_id in event_artists.keys():
            event_details = {
                "date": date["date"],
                "city": cities_dict[city_name],
                "headliner": event_artists[artist_id]["headliner"],
                "name": event_name,
                "start_at": date["start_at"],
                "ticket_url": ticket_link,
                "venue_id": venue_id
            }
            events_to_import[artist_id] = event_details
            print(f"{event_details}\n")
        
    insert_events(events_to_import)
    
if __name__ == "__main__":
    asyncio.run(main())

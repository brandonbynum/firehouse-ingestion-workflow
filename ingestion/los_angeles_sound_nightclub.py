import asyncio
from datetime import datetime
from bs4 import BeautifulSoup
import os
import requests
import sys
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from models import *
from utilities.request_html import request_html
from utilities.insert_events import insert_events
from utilities.convert_str_12hr_to_24hr import convert_str_12hr_to_24hr
from utilities.insert_events import insert_events
from utilities.request_html_as_soup import request_html_as_soup

def extract_and_convert_date(soup: BeautifulSoup) -> dict:
    """
    This function converts date from 'Fri, 1 Jul, 10:00' format to 'YYYY-MM-DD 00:00:00' format
    Args:
        soup (BeautifulSoup): _description_
    Returns:
        Dictionary
    """
    date_text = soup.select('div[class*="EventDetailsTitle__Date"]')[0].text
    date_split = [x.strip() for x in date_text.split(",")]
    date_split_month = date_split[1].split(' ')[1].strip()
    date_split_month_num = datetime.strptime(date_split_month, "%b").month
    
    today = datetime.today()
    todays_month = today.month
    todays_year = today.year
    event_year = str(todays_year) if todays_month >= date_split_month_num  else str(todays_year + 1)
    date_month = date_split[1] + f" {event_year}"
    event_date = str(datetime.strptime(date_month, "%d %b %Y"))
    return {
        "date": event_date, 
        "time": convert_str_12hr_to_24hr(date_split[2])
    }
    
#TODO: Turn prints into logs, and test if they print in Actions output window.  
async def main():
    """This function retrieves sound nightclub's event data of relevent artists and stores
        it in the database.
    """
    url = "http://www.soundnightclub.com/"
    list_soup = request_html_as_soup(url)
    event_soups = {}
    events_to_import = {}
    
    try:
        h1_elements = list_soup.find("section").find_all("h1")
    except Exception as e:
        print(f"Error occurred locating <section></section> tags.\nError: {e}\nExiting...")
        exit()
    
    try:
        artists_dict = {artist["name"]: artist["id"] for artist in Artists.get().dicts()}
        venue_details = Venues.get_as_dict(Venues.name == 'Sound Nightclub')
    except Exception as e:
        print(f"Failed to retrieve artists and cities from database.\nError: {e}")
        exit()
    
    for h1 in h1_elements:
        detail_url = h1.parent["href"].replace("https", "http")
        html = requests.get(detail_url).content
        soup = BeautifulSoup(html, features="html.parser")
        event_soups[detail_url] = soup
    
    for url in event_soups.keys():
        soup = event_soups[url]
        artist_names = [name.strip() for name in soup.select('div[class*="EventDetailsLineup__ArtistTitle"]')[0].text.split(",")]
        matching_artists = set(artist_names).intersection(artists_dict.keys())
        
        if len(matching_artists) > 0:
            event_title = ''
            try:
                event_title = soup.select('div[class*="EventDetailsTitle__Title"]')[0].text
            except Exception as e:
                print("Failed to extract event title")
            converted_date = extract_and_convert_date(soup)
            event_details = {
                "date": converted_date["date"],
                "city": venue_details["city_id"],
                "name": event_title,
                "start_at": converted_date["time"],
                "ticket_url": url,
                "venue_id": venue_details["id"]
            }
            for artist in matching_artists:
                events_to_import[artists_dict[artist]] = event_details
           
    insert_events(events_to_import)

if __name__ == "__main__":
    asyncio.run(main())
from bs4 import BeautifulSoup
import requests

def request_html_as_soup(url: str) -> BeautifulSoup:
    """
    This function retrieves url's html and converts to BeautifulSoup object.
    Args:
        url (str): Location of html
    Returns:
        BeautifulSoup: BS4 object of HTML
    """
    try:
        html = requests.get(url).content
    except Exception as e:
        print(f"\tFailed to retrieve content: ", e)
        exit()
    soup = BeautifulSoup(html, features="html.parser")
    return soup
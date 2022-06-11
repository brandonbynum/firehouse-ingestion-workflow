import requests
from bs4 import BeautifulSoup
from random import choice

def proxy_generator():
    response = requests.get("https://sslproxies.org/")
    soup = BeautifulSoup(response.content, 'html.parser')
    proxy = {'https': choice(list(zip(
                            map(lambda x:x.text, soup.findAll('td')[::8]), 
                            map(lambda x:x.text, soup.findAll('td')[1::8])
                        )))}
    return proxy

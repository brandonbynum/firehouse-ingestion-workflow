import requests
from utilities.proxy_generator import proxy_generator

def data_scraper(request_method, url, **kwargs):
    while True:
        try:
            proxy = proxy_generator()
            response = requests.request(request_method, url, proxies=proxy, timeout=7, **kwargs)
            break
            # if the request is successful, no exception is raised
        except:
            print("Connection error, looking for another proxy")
            pass
    return response

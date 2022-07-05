from requests import request
from utilities.proxy_generator import proxy_generator

def request_html(url, **kwargs):
    total_retries = 5
    retries = 0
    
    while retries < total_retries:
        try:
            proxy = proxy_generator()
            response = request("get", url, proxies=proxy, timeout=7, **kwargs)
            break
            # if the request is successful, no exception is raised
        except Exception as inst:
            if retries == total_retries - 1:
                print("Max retry attempts failed...")
                exit()
            else:
                print("Connection error, looking for another proxy")
                print(f"\t{inst}\n")

                retries += 1
            pass
        
    return response.content

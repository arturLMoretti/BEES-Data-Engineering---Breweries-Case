import json
import os
import time
import requests
import datetime

BASE_URL = "https://api.openbrewerydb.org/v1/breweries"

class BreweryAPIClient:
    '''
    Class responsible for making requests to the OpenBrewery API
    '''
    
    def __init__(self, base_url: str = BASE_URL):
        self.base_url = base_url
    
    def get_brewery_data(self, page: int, per_page: int):
        '''
        Get a list of breweries from openbrewerydb for a specific page
        '''
        try:
            response = requests.get(f"{self.base_url}?per_page={per_page}&page={page}", timeout=5)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error getting brewery data: {e}")
            return []
    
    def get_total_data_count(self):
        '''
        Get the total number of breweries from openbrewerydb
        '''
        try:
            response = requests.get(f"{self.base_url}/meta", timeout=5)
            response.raise_for_status()
            data = response.json()
            return data.get("total", 0)
        except Exception as e:
            print(f"Error getting total data count: {e}")
            return 0


class BreweryDataSaver:
    '''
    Class responsible for saving brewery data to files
    '''
    
    def __init__(self, base_dir: str = "./data/bronze"):
        self.base_dir = base_dir
        self.dir_path = os.path.join(base_dir, f"ingested_at_{datetime.datetime.now().strftime('%Y%m%d')}")
        os.makedirs(self.dir_path, exist_ok=True)
    
    def save_brewery_data(self, data: list, page: int):
        '''
        Save brewery data to a json file
        '''
        timestamp = datetime.datetime.now().strftime("%H%M%S")
        file_path = os.path.join(self.dir_path, f"{timestamp}-{page}.json")
        try:
            with open(file_path, "w") as f:
                json.dump(data, f, indent=2)
            print(f"Saved brewery data to {file_path}")
        except Exception as e:
            print(f"Error saving brewery data: {e}")


def get_all_breweries(per_page: int = 200, total_breweries: int = 0):
    '''
    Orchestrates the data extraction process using the API client and data saver
    '''
    api_client = BreweryAPIClient()
    data_saver = BreweryDataSaver()
    
    page = 1
    total_requests = (total_breweries // per_page) + (1 if total_breweries % per_page > 0 else 0)
    
    try:
        while page <= total_requests:
            print(f"Getting page {page}")
            data = api_client.get_brewery_data(page=page, per_page=per_page)
            if len(data) == 0:
                print("No data returned, stopping")
                break
            print(f"Retrieved {len(data)} breweries from page {page}")
            data_saver.save_brewery_data(data, page)
            page += 1
            time.sleep(1)
    except Exception as e:
        print(f"Error getting all breweries: {e}")

if __name__ == "__main__":
    brewery_per_page = 200
    
    api_client = BreweryAPIClient()
    total_breweries = api_client.get_total_data_count()
    print(f"Total breweries to fetch: {total_breweries}")
    
    if total_breweries == 0:
        print("No breweries found, exiting.")
        exit()

    get_all_breweries(per_page=brewery_per_page, total_breweries=total_breweries)
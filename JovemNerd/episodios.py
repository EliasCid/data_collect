# %%
import requests
import pandas as pd
import datetime
import json
import time

class Collector:
    
    def __init__(self, url, instance_name):
        self.url = url
        self.instance_name = instance_name

    def get_content(self, **kwargs):
        
        reponse = requests.get(url, params=kwargs)
        return reponse

    def save_parquet(self, data):
        now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f")
        df = pd.DataFrame(data)
        df.to_parquet(f"data/{self.instance_name}/parquet/{now}.parquet", index=False)
    
    def save_json(self, data):
        now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f")
        with open (f"data/{self.instance_name}/json/{now}.json", "w") as open_file:
            json.dump(data, open_file, indent=4)

    def save_data(self, data, format='json'):
        if format == 'json':
            self.save_json(data)
        
        elif format == 'parquet':
            self.save_parquet(data)

    def get_and_save(self, save_format='json', **kwargs):
        response = self.get_content(**kwargs)
        if response.status_code == 200:
            data = response.json()
            self.save_data(response.json(), save_format)
        else:
            data = None
            print(f"Request without success: {response.status_code}", response.json())
        return data
    
    def auto_exec(self, save_format='json', date_stop='2000-01-01'):
        page = 1
        while True:
            print(page)
            data = self.get_and_save(save_format=save_format,
                                     page=page,
                                     per_page=1000)
            if data == None:
                print("Error while collecting data... waiting.")
                time.sleep(60*5)
            else:
                date_last = pd.to_datetime(data[-1]["published_at"]).date()
                if date_last < pd.to_datetime(date_stop).date():
                    break
                elif len(data) < 1000:
                    break
                page += 1
                time.sleep(5)
                
# %%
url = "https://api.jovemnerd.com.br/wp-json/jovemnerd/v1/nerdcasts/"
collect = Collector(url, "episodios")
collect.auto_exec()
# %%

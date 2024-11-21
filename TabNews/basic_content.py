# %%
import requests
import pandas as pd
import datetime
import json
import time

def get_response(**kwargs):
    url = "https://www.tabnews.com.br/api/v1/contents"
    response = requests.get(url, params=kwargs)
    return response

def save_data(data, option='json'):

    now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f")
    
    if option == 'json':
        with open (f"data/contents/json/{now}.json", "w") as open_file:
            json.dump(data, open_file, indent=4)

    elif option == 'dataframe':
        df = pd.DataFrame(data)
        df.to_parquet(f"data/contents/parquet/{now}.parquet", index=False)
# %%

page = 1
date_stop = pd.to_datetime('2024-01-01').date()

while True:
    print(page)
    response = get_response(page=page, per_page=100, strategy="new")
    if response.status_code == 200:
        data = response.json()
        save_data(data)

        date = pd.to_datetime(data[-1]["update_at"]).date()

        if len(data) < 100 or date < date_stop:
            break

        page +=1
        time.sleep(5)

    else:
        print(response.status_code)
        print(response.json())
        time.sleep(60 * 15)
# %%

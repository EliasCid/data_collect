# %%
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

#We get this info from CURL to Python (https://curlconverter.com/python/)
headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'es-419,es;q=0.9,es-ES;q=0.8,en;q=0.7,en-GB;q=0.6,en-US;q=0.5,es-MX;q=0.4,pt-BR;q=0.3,pt;q=0.2',
    'cache-control': 'max-age=0',
    'priority': 'u=0, i',
    'referer': 'https://www.residentevildatabase.com/personagens/',
    'sec-ch-ua': '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
}

def get_content(url):
    response = requests.get(url, headers=headers)
    return response

def get_basic_infos(soup):
    div_page = soup.find("div", class_ ="td-page-content") #We get data from divs
    paragraph = div_page.find_all("p")[1] #Then we separate them using "p"
    ems = paragraph.find_all("em") #Then we separate them using "em"

    #We create a dictionary
    data = {}
    for i in ems:
        key, value, *_ = i.text.split(":")
        key = key.strip(" ")
        data[key] = value

    return data

def get_aparitions(soup):
    #Get information below H4:
    list_ref = (soup.find("div", class_ ="td-page-content")
                    .find("h4")
                    .find_next()
                    .find_all("li"))
    aparitions = [i.text for i in list_ref]
    return aparitions

def get_character_infos(url):
    response = get_content(url)
    if response.status_code != 200:
        print("No response could be obtained")
        return{}
    else:
        soup = BeautifulSoup(response.text)
        data = get_basic_infos(soup)
        data["Aparitions"] = get_aparitions(soup)
        return data

def get_links():
    url = "https://www.residentevildatabase.com/personagens/"
    response = requests.get(url, headers=headers)
    soup_characters = BeautifulSoup(response.text)
    character_links = (soup_characters.find("div", class_="td-page-content")
                                    .find_all("a"))

    links = [i["href"] for i in character_links]
    return links
# %%
links = get_links()
data = []
for i in tqdm(links):
    d = get_character_infos(i)
    d["Link"] = i
    name = i.strip("/").split("/")[-1].replace("-", " ").title()
    d["Name"] = name
    data.append(d)
# %%
df = pd.DataFrame(data)
df.to_parquet("data_re.parquet", index=False)
# %%

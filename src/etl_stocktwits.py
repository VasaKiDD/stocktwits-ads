import requests
import time
import random
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm


def extract_twits(symbol, n_pages, last_id=None, sleep_scale=1.0):
    url = "https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json?max="
    url = url.format(symbol=symbol)
    if last_id == None:
        last_id = "10000000000"
    data = []
    for _ in tqdm(range(n_pages)):
        data.extend(requests.get(url + last_id).json()["messages"])
        last_id = str(data[-1]["id"])
        time.sleep(random.random() * sleep_scale)
    return data


def transform_data(data):
    twits = []
    for message in tqdm(data):
        twit = {
            "id": message["id"],
            "symbol": message["symbols"][0]["symbol"],
            "user": message["user"]["username"],
            "user_followers": message["user"]["followers"],
            "date": pd.to_datetime(message["created_at"]),
            "content": message["body"],
        }
        if message["entities"]["sentiment"]:
            sentiment = message["entities"]["sentiment"]["basic"]
            if sentiment == "Bullish":
                sentiment_int = 1.0
            elif sentiment == "Bearish":
                sentiment_int = -1.0
            else:
                sentiment = ""
                sentiment_int = 0.0
        else:
            sentiment = ""
            sentiment_int = 0.0
        twit["sentiment"] = sentiment
        twit["sentiment_int"] = sentiment_int
        twits.append(twit)

    return twits


def push_data_and_verify(
    data, url, db_name, collection_name, full_verif=False, first=False
):
    client = MongoClient(url)
    db = client[db_name]
    collection = db[collection_name]
    for twit in tqdm(data):
        if not first:
            exist = collection.find_one({"id": twit["id"]})
            if exist is None:
                collection.insert_one(twit)
                if not full_verif:
                    break
        else:
            try:
                collection.insert_one(twit)
            except:
                import pdb

                pdb.set_trace()

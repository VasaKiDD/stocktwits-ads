import datetime
from pymongo import MongoClient
import pandas as pd
from src.extract_yahoo import extract_yahoo


def put_prices_in_twits(symbol, twits, prices):
    key_ = symbol + "_price"
    for twit in twits:
        twit_date = twit["date"].date()
        closest = prices.truncate(before=twit_date).iloc[0]
        closest_date = closest.name.date()
        if closest_date == twit_date:
            twit[key_] = closest["Close"]
        elif closest_date > twit_date:
            twit[key_] = closest["Open"]
        else:
            import pdb

            pdb.set_trace()
    return twits


def create_data_with_prices(start_date, symbol, url, db_name, collection_name):
    client = MongoClient(url)
    db = client[db_name]
    collection = db[collection_name]

    today = datetime.date.today().isoformat()
    prices_symbol = extract_yahoo(symbol, start_date, today)

    results = collection.find(
        {
            "symbol": symbol,
            "date": {"$gt": datetime.datetime.fromisoformat(start_date)},
        }
    )
    twits = [res for res in results]
    twits = put_prices_in_twits(symbol, twits, prices_symbol)
    results = pd.DataFrame(twits)
    results = results.set_index("id")

    return results

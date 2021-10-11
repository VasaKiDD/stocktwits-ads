import argparse
from pymongo import MongoClient
import src.etl_stocktwits as etl1

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--recent", action="store_true")

    parser.add_argument("--oldest", action="store_true")

    parser.add_argument("--first", action="store_true")

    args = parser.parse_args()

    # uri = "mongodb+srv://quantboss:ftd2021@flash.4rznu.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    uri = "mongodb+srv://quantboss:ftd2021@flash.4rznu.mongodb.net/"

    if args.first:
        data = etl1.extract_twits("BA", 1, last_id=None, sleep_scale=1.0)
        twits = etl1.transform_data(data)
        etl1.push_data_and_verify(
            twits, uri, "stocktwits", "boeing", full_verif=True, first=True
        )

    if args.oldest:
        client = MongoClient(uri)
        db = client["stocktwits"]
        collection = db["boeing"]
        db.collection.find({"name": "John"}).sort({"date_time": 1}).limit(1)

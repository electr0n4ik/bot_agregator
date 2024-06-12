import os
import asyncio
import dotenv

from datetime import datetime, timedelta
from motor import motor_asyncio
from collections import defaultdict


class MongoDBLoader:
    def __init__(self, db_url, db_name, collection_name):
        self.client = motor_asyncio.AsyncIOMotorClient(db_url)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    async def load_data(self, dt_from, dt_upto):
        cursor = self.collection.find({
            "dt": {
                "$gte": dt_from,
                "$lt": dt_upto
            }
        })
        data = await cursor.to_list(length=None)
        return data
    
dotenv.load_dotenv()

MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
MONGODB_DB = os.getenv("MONGODB_DB", "database")
MONGODB_COLL = os.getenv("MONGODB_COLL", "collection")

input_data = {
    "dt_from": "2022-09-01T00:00:00", 
    "dt_upto": "2022-12-31T23:59:00", 
    "group_type": "month"}

async def main():
    dataset = []
    labels = []
    dt_from = datetime.fromisoformat(input_data["dt_from"])
    dt_upto = datetime.fromisoformat(input_data["dt_upto"])
    current = dt_from
    group_type = input_data["group_type"]
    aggregated_data = defaultdict(int)

    mongodb_loader = MongoDBLoader(MONGODB_URL, MONGODB_DB, MONGODB_COLL)
    data = await mongodb_loader.load_data(dt_from, dt_upto)

    if group_type == 'hour':
        date_format = "%Y-%m-%dT%H:00:00"
    elif group_type == 'day':
        date_format = "%Y-%m-%dT00:00:00"
    elif group_type == 'month':
        date_format = "%Y-%m-01T00:00:00"
    
    for entry in data:
        dt = entry['dt']
        value = entry['value']
        
        if group_type in ['hour', 'day']:
            key = dt.strftime(date_format)
        elif group_type == 'month':
            key = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            key = key.strftime(date_format)

        aggregated_data[key] += value
    
    while current < dt_upto:
        key = current.strftime(date_format)
        dataset.append(aggregated_data[key])
        labels.append(key)
        if current.month == 12:
            current = current.replace(year=current.year+1, month=1)
        else:
            current = current.replace(month=current.month+1)

        print("dataset", aggregated_data[key], "labels", key)

asyncio.run(main())


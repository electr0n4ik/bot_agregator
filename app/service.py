import json

from datetime import datetime, timedelta
from motor import motor_asyncio
from collections import defaultdict


class MongoDBLoader:
    """
    Класс отвечает за подключение к MongoDB и выгрузку данных из коллекции.
    """
    def __init__(self, db_url, db_name, collection_name):
        self.client = motor_asyncio.AsyncIOMotorClient(db_url)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    async def load_data(self, dt_from, dt_upto):
        cursor = self.collection.find({
            "dt": {
                "$gte": dt_from,
                "$lte": dt_upto
            }
        })
        data = await cursor.to_list(length=None)
        return data


async def get_aggregate_data(message_text, MONGODB_URL, MONGODB_DB, MONGODB_COLL):
    dataset = []
    labels = []
    delta = None
    aggregated_data = defaultdict(int)

    mongodb_loader = MongoDBLoader(MONGODB_URL, MONGODB_DB, MONGODB_COLL)

    try:
        json_data = json.loads(message_text)
        dt_from = json_data["dt_from"]
        dt_upto = json_data["dt_upto"]
        group_type = json_data["group_type"]
    except:
        return """
Допустимо отправлять только следующие запросы:
{"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}
{"dt_from": "2022-10-01T00:00:00", "dt_upto": "2022-11-30T23:59:00", "group_type": "day"}
{"dt_from": "2022-02-01T00:00:00", "dt_upto": "2022-02-02T00:00:00", "group_type": "hour"}"""
    
    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)
    current = dt_from
    data = await mongodb_loader.load_data(dt_from, dt_upto)

    if group_type == 'hour':
        delta = timedelta(hours=1)
        date_format = "%Y-%m-%dT%H:00:00"
    elif group_type == 'day':
        delta = timedelta(days=1)
        date_format = "%Y-%m-%dT00:00:00"
    elif group_type == 'month':
        date_format = "%Y-%m-01T00:00:00"
    else:
        raise ValueError("Unsupported group_type. Use 'hour', 'day', or 'month'.")

    for entry in data:
        dt = entry['dt']
        value = entry['value']
        
        if group_type in ['hour', 'day']:
            key = dt.strftime(date_format)
        elif group_type == 'month':
            key = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            key = key.strftime(date_format)

        aggregated_data[key] += value
    
    if group_type == 'month':
        while current < dt_upto:
            key = current.strftime(date_format)
            dataset.append(aggregated_data.get(key, 0))
            labels.append(key)
            if current.month == 12:
                current = current.replace(year=current.year+1, month=1)
            else:
                current = current.replace(month=current.month+1)
    else:
        while current < dt_upto:
            key = current.strftime(date_format)
            dataset.append(aggregated_data.get(key, 0))
            labels.append(key)
            current += delta
    
    return {"dataset": dataset, "labels": labels}

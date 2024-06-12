import os
import asyncio
import dotenv

from service import main

dotenv.load_dotenv()

MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
MONGODB_DB = os.getenv("MONGODB_DB", "database")
MONGODB_COLL = os.getenv("MONGODB_COLL", "collection")

input_data = {
    "dt_from": "2022-09-01T00:00:00", 
    "dt_upto": "2022-12-31T23:59:00", 
    "group_type": "month"}

print(asyncio.run(main(input_data, MONGODB_URL, MONGODB_DB, MONGODB_COLL)))

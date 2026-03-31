#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymongo
from pymongo import MongoClient

MONGO_CONFIG = {
    "host": "10.34.137.87",
    "port": 37018,
    "username": "nodeDayOpsWide_r",
    "password": "pRkuawfIRKXkRQu1nhTLYhjF96QpAyXXYou",
    "auth_db": "jarvis",
    "db": "jarvis",
    "collection": "nodeDayOpsWide",
}

client = MongoClient(
    host=MONGO_CONFIG["host"],
    port=MONGO_CONFIG["port"],
    username=MONGO_CONFIG["username"],
    password=MONGO_CONFIG["password"],
    authSource=MONGO_CONFIG["auth_db"]
)
db = client[MONGO_CONFIG["db"]]
collection = db[MONGO_CONFIG["collection"]]

pipeline = [
    {
        "$group": {
            "_id": "$day",
            "count": {"$sum": 1}
        }
    },
    {"$sort": {"_id": 1}}
]

result = collection.aggregate(pipeline)
print("日期         | 数据量")
print("--------------|-------")
total = 0
for doc in result:
    date = doc["_id"]
    count = doc["count"]
    total += count
    print(f"{date} | {count:,}")

print("--------------|-------")
print(f"**总计**     | {total:,}")

client.close()

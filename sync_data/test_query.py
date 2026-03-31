#!/usr/bin/env python3
import pymongo
from pymongo import MongoClient
from bson import ObjectId

print("开始连接MongoDB...")
c = MongoClient(
    host='10.34.137.87', 
    port=37018, 
    username='nodeDayOpsWide_r', 
    password='pRkuawfIRKXkRQu1nhTLYhjF96QpAyXXYou', 
    authSource='jarvis',
    serverSelectionTimeoutMS=10000
)
print("连接成功")
coll = c['jarvis']['nodeDayOpsWide']

print("查询10条数据...")
query = {"_id": {"$gt": ObjectId("69b89989c5b08797316f6311")}}
cursor = coll.find(query).sort("_id", 1).limit(10)
for doc in cursor:
    print(f"ID: {doc['_id']}, updatedTime: {doc.get('updatedTime')}")

print("查询完成")
c.close()

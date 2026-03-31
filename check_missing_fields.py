#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymongo
from pymongo import MongoClient

# MongoDB 配置
MONGO_CONFIG = {
    "host": "10.34.137.87",
    "port": 37018,
    "username": "nodeDayOpsWide_r",
    "password": "pRkuawfIRKXkRQu1nhTLYhjF96QpAyXXYou",
    "auth_db": "jarvis",
    "db": "jarvis",
    "collection": "nodeDayOpsWide",
}

# 需要检查的遗漏字段
missing_fields = [
    "idcId",
    "idcBandwidth", 
    "transProvRate",
    "isVm",
    "isCloudVm",
    "scheduleISPs",
    "settlePeriodType",
]

# 连接 MongoDB
print("=== 检查 MongoDB 中遗漏字段的存在情况 ===\n")
mongo_client = MongoClient(
    host=MONGO_CONFIG["host"],
    port=MONGO_CONFIG["port"],
    username=MONGO_CONFIG["username"],
    password=MONGO_CONFIG["password"],
    authSource=MONGO_CONFIG["auth_db"]
)
mongo_db = mongo_client[MONGO_CONFIG["db"]]
collection = mongo_db[MONGO_CONFIG["collection"]]

total_count = collection.count_documents({})
print(f"总记录数: {total_count:,}\n")

print("字段名称        | 存在记录数  |  占比  | 示例值")
print("----------------|-------------|-------|----------")
for field in missing_fields:
    query = {field: {"$exists": True, "$ne": None}}
    count = collection.count_documents(query)
    percentage = count * 100.0 / total_count
    
    # 获取一个示例值
    sample_doc = collection.find_one(query)
    sample_val = str(sample_doc.get(field)) if sample_doc else "N/A"
    if len(sample_val) > 30:
        sample_val = sample_val[:27] + "..."
    
    print(f"{field:<14} | {count:>10,} | {percentage:>5.2f}% | {sample_val}")

# 按天统计几个关键字段
print("\n=== 按天统计主要遗漏字段的存在情况 ===")
print("\n日期         | idcId 存在 | isVm 存在 | isCloudVm 存在")
print("--------------|-----------|-----------|---------------")
pipeline = [
    {
        "$group": {
            "_id": "$day",
            "idcId_count": {"$sum": {"$cond": [{"$exists": ["$idcId", True]}, 1, 0]}},
            "isVm_count": {"$sum": {"$cond": [{"$exists": ["$isVm", True]}, 1, 0]}},
            "isCloudVm_count": {"$sum": {"$cond": [{"$exists": ["$isCloudVm", True]}, 1, 0]}},
        }
    },
    {"$sort": {"_id": 1}}
]

result = collection.aggregate(pipeline)
for doc in result:
    day = doc["_id"]
    print(f"{day:<12} | {doc['idcId_count']:>9} | {doc['isVm_count']:>9} | {doc['isCloudVm_count']:>13}")

mongo_client.close()

#!/usr/bin/env python3
import pymongo
from pymongo import MongoClient
import pymysql
import json
import os
from bson import ObjectId, Decimal128, Int64
from datetime import datetime, timedelta

MONGO_CONFIG = {
    "host": "10.34.137.87",
    "port": 37018,
    "username": "nodeDayOpsWide_r",
    "password": "pRkuawfIRKXkRQu1nhTLYhjF96QpAyXXYou",
    "auth_db": "jarvis",
    "db": "jarvis",
    "collection": "nodeDayOpsWide",
    "batch_size": 1000,
    "incremental_field": "updatedTime",
}

STARROCKS_CONFIG = {
    "host": "10.70.33.22",
    "port": 9030,
    "user": "srtest",
    "password": "srtest@890",
    "db": "test",
    "table": "node_day_ops_wide",
}

CHECKPOINT_FILE = "./sync_checkpoint.json"

def to_datetime(val):
    if isinstance(val, datetime):
        return val
    elif isinstance(val, (int, float)):
        if val > 1e12:
            val = val / 1000
        return datetime.fromtimestamp(val)
    elif isinstance(val, str):
        if len(val) == 23:
            return datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f")
        elif len(val) == 19:
            return datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
        else:
            return datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f")
    else:
        raise ValueError(f"无法转换为时间: {type(val)} = {val}")

def bson_to_json_serializable(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, Int64) or type(obj).__name__ == "Int32":
        return int(obj)
    elif isinstance(obj, Decimal128):
        return obj.to_decimal()
    elif isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    elif isinstance(obj, (list, dict)):
        try:
            return json.dumps(obj, default=bson_to_json_serializable, ensure_ascii=False)
        except ValueError:
            return str(obj)
    else:
        return obj

def flatten_doc(doc, prefix=""):
    flat = {}
    for k, v in doc.items():
        key = f"{prefix}{k}" if prefix else k
        if isinstance(v, dict):
            flat.update(flatten_doc(v, key + "_"))
        elif isinstance(v, (list, dict)):
            flat[key] = bson_to_json_serializable(v)
        else:
            flat[key] = bson_to_json_serializable(v)
    return flat

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_id": None, "last_updated_time": None, "sync_type": "full"}

def save_checkpoint(checkpoint):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=2)

def get_sr_connection():
    return pymysql.connect(
        host=STARROCKS_CONFIG["host"],
        port=STARROCKS_CONFIG["port"],
        user=STARROCKS_CONFIG["user"],
        password=STARROCKS_CONFIG["password"],
        database=STARROCKS_CONFIG["db"],
        charset="utf8mb4",
        autocommit=False,
    )

def get_mongo_collection():
    client = MongoClient(
        host=MONGO_CONFIG["host"],
        port=MONGO_CONFIG["port"],
        username=MONGO_CONFIG["username"],
        password=MONGO_CONFIG["password"],
        authSource=MONGO_CONFIG["auth_db"],
        serverSelectionTimeoutMS=10000,
        connectTimeoutMS=10000,
    )
    db = client[MONGO_CONFIG["db"]]
    return db[MONGO_CONFIG["collection"]]

def sync_full():
    print("=== 开始全量同步 ===")
    checkpoint = load_checkpoint()
    coll = get_mongo_collection()
    sr_conn = get_sr_connection()
    sr_cursor = sr_conn.cursor()

    query = {}
    if checkpoint["last_id"]:
        query["_id"] = {"$gt": ObjectId(checkpoint["last_id"])}
        print(f"从断点继续，最后ID: {checkpoint['last_id']}")

    fields = [
        "customerId",
        "day",
        "nodeId",
        "analyzePeak95",
        "baseInfo_channelId",
        "baseInfo_signatoryId",
        "baseInfo_bandwidth",
        "buildBandwidth",
        "city",
        "cost_guaranteedRate",
        "cost_priceItemId",
        "cost_priceItemName",
        "cost_priceType",
        "cost_price",
        "cost_priceAfterBonus",
        "cost_measure",
        "cost_original",
        "cost_bonus",
        "cost_slaDeduction",
        "cost_tobaDeduction",
        "cost_settlement",
        "cost_adjustmentAmount",
        "cost_finalAmount",
        "customerName",
        "deliveryType",
        "evening20To23Avg",
        "eveningAvg",
        "eveningPeak95",
        "isBanTransProv",
        "isp",
        "name",
        "natType",
        "nodeTags",
        "nodeType",
        "os",
        "peak95",
        "peak95Ratio",
        "peak95Time",
        "peakMaxRatio",
        "priceNumber",
        "profit_profitAmount",
        "profit_profitRate",
        "profit_estimatedProfitAmount",
        "profit_estimatedProfitRate",
        "province",
        "purchaserName",
        "quantityEnd",
        "quantityType",
        "realISP",
        "resourceType",
        "revenue_guaranteedRate",
        "revenue_priceItemId",
        "revenue_priceItemName",
        "revenue_price",
        "revenue_measure",
        "revenue_coefficientMeasure",
        "revenue_amount",
        "revenue_finalAmount",
        "revenue_estimatedFinalAmount",
        "signatoryName",
        "snapshotTime",
        "stage",
        "stairType",
        "stairs",
        "state",
        "tcpNatType",
        "udpNatType",
        "unEveningAvg",
        "updatedTime",
        "vendorId",
        "vendorSuggestCustomers",
        "virtualCustomers",
        "webPort",
        "webPortResult",
    ]

    placeholder = ", ".join(["%s"] * len(fields))
    insert_sql = f"INSERT INTO {STARROCKS_CONFIG['table']} ({', '.join(fields)}) VALUES ({placeholder})"

    count = 0
    last_id = checkpoint["last_id"]
    last_updated_time = checkpoint["last_updated_time"]
    batch = []
    batch_size = MONGO_CONFIG["batch_size"]

    print("开始查询MongoDB数据...")
    cursor = coll.find(query).sort("_id", 1).batch_size(batch_size)
    print("查询成功，开始同步...")
    for doc in cursor:
        flat_doc = flatten_doc(doc)
        row = [flat_doc.get(field, None) for field in fields]
        batch.append(row)

        count += 1
        last_id = str(doc["_id"])
        last_updated_time = bson_to_json_serializable(
            doc.get(MONGO_CONFIG["incremental_field"])
        )

        if len(batch) == batch_size:
            sr_cursor.executemany(insert_sql, batch)
            sr_conn.commit()
            batch.clear()
            checkpoint.update(
                {
                    "last_id": last_id,
                    "last_updated_time": last_updated_time,
                    "sync_type": "full",
                }
            )
            save_checkpoint(checkpoint)
            print(f"已同步 {count} 条，断点已保存，最后ID: {last_id}")

    if batch:
        sr_cursor.executemany(insert_sql, batch)
        sr_conn.commit()
        print(f"已同步剩余 {len(batch)} 条")

    checkpoint.update(
        {
            "last_id": last_id,
            "last_updated_time": last_updated_time,
            "sync_type": "incremental",
        }
    )
    save_checkpoint(checkpoint)
    print(f"全量同步完成，共同步 {count} 条数据，已切换到增量模式")

    sr_cursor.close()
    sr_conn.close()

if __name__ == "__main__":
    sync_full()

#!/usr/bin/env python3
import csv
import pymongo
from pymongo import MongoClient
from bson import ObjectId, Decimal128, Int64
from datetime import datetime

MONGO_CONFIG = {
    "host": "10.34.137.87",
    "port": 37018,
    "username": "nodeDayOpsWide_r",
    "password": "pRkuawfIRKXkRQu1nhTLYhjF96QpAyXXYou",
    "auth_db": "jarvis",
    "db": "jarvis",
    "collection": "nodeDayOpsWide",
    "batch_size": 10000,
}

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

def bson_to_str(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, Int64) or type(obj).__name__ == "Int32":
        return int(obj)
    elif isinstance(obj, Decimal128):
        return str(obj.to_decimal())
    elif isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    elif isinstance(obj, (list, dict)):
        import json
        try:
            return json.dumps(obj, ensure_ascii=False)
        except:
            return str(obj)
    elif obj is None:
        return ""
    else:
        return str(obj)

def flatten_doc(doc, prefix=""):
    flat = {}
    for k, v in doc.items():
        key = f"{prefix}{k}" if prefix else k
        if isinstance(v, dict):
            flat.update(flatten_doc(v, key + "_"))
        else:
            flat[key] = bson_to_str(v)
    return flat

if __name__ == "__main__":
    client = MongoClient(
        host=MONGO_CONFIG["host"],
        port=MONGO_CONFIG["port"],
        username=MONGO_CONFIG["username"],
        password=MONGO_CONFIG["password"],
        authSource=MONGO_CONFIG["auth_db"],
    )
    coll = client[MONGO_CONFIG["db"]][MONGO_CONFIG["collection"]]
    total = coll.count_documents({})
    print(f"总数据量: {total} 条，开始导出...")
    
    with open("mongo_data.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        count = 0
        cursor = coll.find().sort("_id", 1).batch_size(MONGO_CONFIG["batch_size"])
        for doc in cursor:
            flat = flatten_doc(doc)
            row = [flat.get(field, "") for field in fields]
            writer.writerow(row)
            count += 1
            if count % 100000 == 0:
                print(f"已导出 {count} 条，进度: {count/total*100:.2f}%")
    
    print(f"导出完成，共 {count} 条")

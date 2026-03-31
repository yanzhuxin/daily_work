#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymongo
from pymongo import MongoClient
import pymysql
import json
import os
import decimal
from bson import ObjectId, Decimal128, Int64
from datetime import datetime, timedelta

# 配置
MONGO_CONFIG = {
    "host": "10.34.137.87",
    "port": 37018,
    "username": "nodeDayOpsWide_r",
    "password": "pRkuawfIRKXkRQu1nhTLYhjF96QpAyXXYou",
    "auth_db": "jarvis",
    "db": "jarvis",
    "collection": "nodeDayOpsWide",
    "batch_size": 10000,
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

# 断点文件路径
CHECKPOINT_FILE = "./sync_checkpoint.json"


def to_datetime(val):
    """任意类型转datetime对象"""
    if isinstance(val, datetime):
        return val
    elif isinstance(val, (int, float)):
        # 时间戳格式，支持秒和毫秒
        if val > 1e12:
            val = val / 1000
        return datetime.fromtimestamp(val)
    elif isinstance(val, str):
        # 字符串格式时间
        if len(val) == 23:  # 带毫秒的格式 %Y-%m-%d %H:%M:%S.%f
            return datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f")
        elif len(val) == 19:  # 不带毫秒的格式 %Y-%m-%d %H:%M:%S
            return datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
        else:
            return datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f")
    else:
        raise ValueError(f"无法转换为时间: {type(val)} = {val}")


def bson_to_json_serializable(obj):
    """处理MongoDB特殊类型"""
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, Int64) or type(obj).__name__ == "Int32":
        # 兼容不同版本bson的Int32类型，不需要导入直接通过类型名判断
        return int(obj)
    elif isinstance(obj, Decimal128):
        return obj.to_decimal()
    elif isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    elif isinstance(obj, (list, dict)):
        try:
            return json.dumps(obj, default=bson_to_json_serializable, ensure_ascii=False)
        except ValueError:
            # 处理循环引用
            return str(obj)
    else:
        return obj


def flatten_doc(doc, prefix=""):
    """嵌套文档展开为扁平结构"""
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
    """加载上次同步断点"""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_id": None, "last_updated_time": None, "sync_type": "full"}


def save_checkpoint(checkpoint):
    """保存同步断点"""
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=2)


def get_sr_connection():
    """获取StarRocks连接"""
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
    """获取MongoDB集合"""
    client = MongoClient(
        host=MONGO_CONFIG["host"],
        port=MONGO_CONFIG["port"],
        username=MONGO_CONFIG["username"],
        password=MONGO_CONFIG["password"],
        authSource=MONGO_CONFIG["auth_db"],
    )
    db = client[MONGO_CONFIG["db"]]
    return db[MONGO_CONFIG["collection"]]


def sync_full():
    """全量同步"""
    print("=== 开始全量同步 ===")
    checkpoint = load_checkpoint()
    coll = get_mongo_collection()
    sr_conn = get_sr_connection()
    sr_cursor = sr_conn.cursor()

    # 构建查询条件
    query = 
# 分片查询条件注入
query["_id"] = {"$gte": ObjectId("69ba96dd45b08797319ec1e1"), "$lt": ObjectId("69bb95df85b0879731bc710d")}

    if checkpoint["last_id"]:
        query["_id"] = {"$gt": ObjectId(checkpoint["last_id"])}

    # 字段映射，和建表SQL顺序一致
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

    try:
        cursor = coll.find(query).sort("_id", 1).batch_size(batch_size)
        for doc in cursor:
            flat_doc = flatten_doc(doc)
            # 补全缺失字段为None
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

        # 插入剩余数据
        if batch:
            sr_cursor.executemany(insert_sql, batch)
            sr_conn.commit()
            print(f"已同步剩余 {len(batch)} 条")
        # 全量同步完成，更新断点为增量模式
        checkpoint.update(
            {
                "last_id": last_id,
                "last_updated_time": last_updated_time,
                "sync_type": "incremental",
            }
        )
        save_checkpoint(checkpoint)
        print(f"全量同步完成，共同步 {count} 条数据，已切换到增量模式")

    except Exception as e:
        sr_conn.rollback()
        print(f"全量同步失败: {str(e)}")
        raise
    finally:
        sr_cursor.close()
        sr_conn.close()


def sync_incremental():
    """增量同步"""
    print("=== 开始增量同步 ===")
    checkpoint = load_checkpoint()
    if checkpoint["sync_type"] != "incremental":
        print("当前不是增量模式，请先完成全量同步")
        return

    coll = get_mongo_collection()
    sr_conn = get_sr_connection()
    sr_cursor = sr_conn.cursor()

    last_updated_time = checkpoint.get("last_updated_time")
    if not last_updated_time:
        last_updated_time = (datetime.now() - timedelta(days=7)).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )[:-3]

    # 增量查询：更新时间大于上次同步时间
    query = {
        MONGO_CONFIG["incremental_field"]: {
            "$gte": datetime.strptime(last_updated_time, "%Y-%m-%d %H:%M:%S.%f")
        }
    }

    # 字段映射和全量一致
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
    # 增量使用INSERT OVERWRITE或者ON DUPLICATE KEY UPDATE，这里用StarRocks的主键更新模式
    insert_sql = f"""
    INSERT INTO {STARROCKS_CONFIG["table"]} ({", ".join(fields)}) 
    VALUES ({placeholder})
    ON DUPLICATE KEY UPDATE 
    {", ".join([f"{field} = VALUES({field})" for field in fields if field not in ["customerId", "day", "nodeId"]])}
    """

    count = 0
    max_updated_time = last_updated_time

    try:
        cursor = (
            coll.find(query)
            .sort(MONGO_CONFIG["incremental_field"], 1)
            .batch_size(MONGO_CONFIG["batch_size"])
        )
        for doc in cursor:
            flat_doc = flatten_doc(doc)
            row = [flat_doc.get(field, None) for field in fields]
            sr_cursor.execute(insert_sql, row)

            count += 1
            current_updated_time_raw = doc[MONGO_CONFIG["incremental_field"]]
            current_updated_time = bson_to_json_serializable(current_updated_time_raw)
            # 统一转成datetime对象比较，避免类型错误
            if to_datetime(current_updated_time_raw) > to_datetime(max_updated_time):
                max_updated_time = current_updated_time

            if count % MONGO_CONFIG["batch_size"] == 0:
                sr_conn.commit()
                print(f"已同步增量 {count} 条")

        sr_conn.commit()
        # 更新增量断点
        checkpoint["last_updated_time"] = max_updated_time
        save_checkpoint(checkpoint)
        print(f"增量同步完成，共同步 {count} 条数据，最新更新时间: {max_updated_time}")

    except Exception as e:
        sr_conn.rollback()
        print(f"增量同步失败: {str(e)}")
        raise
    finally:
        sr_cursor.close()
        sr_conn.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python sync_node_day_ops.py [full|incremental]")
        sys.exit(1)

    mode = sys.argv[1]
    if mode == "full":
        sync_full()
    elif mode == "incremental":
        sync_incremental()
    else:
        print(f"未知模式: {mode}，支持 full 或 incremental")
        sys.exit(1)


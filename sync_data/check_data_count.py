#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymongo
import pymysql
import json
from datetime import datetime

# 配置（直接从同步脚本读取）
MONGO_CONFIG = {
    "host": "10.34.137.87",
    "port": 37018,
    "username": "nodeDayOpsWide_r",
    "password": "pRkuawfIRKXkRQu1nhTLYhjF96QpAyXXYou",
    "auth_db": "jarvis",
    "db": "jarvis",
    "collection": "nodeDayOpsWide",
}

STARROCKS_CONFIG = {
    "host": "10.70.33.22",
    "port": 9030,
    "user": "srtest",
    "password": "srtest@890",
    "db": "test",
    "table": "node_day_ops_wide",
}


def get_mongodb_count():
    """获取MongoDB总记录数"""
    print("=== 连接MongoDB查询总记录数...")
    client = pymongo.MongoClient(
        host=MONGO_CONFIG["host"],
        port=MONGO_CONFIG["port"],
        username=MONGO_CONFIG["username"],
        password=MONGO_CONFIG["password"],
        authSource=MONGO_CONFIG["auth_db"]
    )
    db = client[MONGO_CONFIG["db"]]
    collection = db[MONGO_CONFIG["collection"]]
    count = collection.count_documents({})
    client.close()
    return count


def get_starrocks_count():
    """获取StarRocks总记录数"""
    print("=== 连接StarRocks查询总记录数...")
    conn = pymysql.connect(
        host=STARROCKS_CONFIG["host"],
        port=STARROCKS_CONFIG["port"],
        user=STARROCKS_CONFIG["user"],
        password=STARROCKS_CONFIG["password"],
        database=STARROCKS_CONFIG["db"],
        connect_timeout=30
    )
    cursor = conn.cursor()
    sql = f"SELECT COUNT(*) FROM {STARROCKS_CONFIG['db']}.{STARROCKS_CONFIG['table']}"
    cursor.execute(sql)
    count = cursor.fetchone()[0]
    conn.close()
    return count


def get_mongodb_count_by_day():
    """按day分组统计MongoDB"""
    client = pymongo.MongoClient(
        host=MONGO_CONFIG["host"],
        port=MONGO_CONFIG["port"],
        username=MONGO_CONFIG["username"],
        password=MONGO_CONFIG["password"],
        authSource=MONGO_CONFIG["auth_db"]
    )
    db = client[MONGO_CONFIG["db"]]
    collection = db[MONGO_CONFIG["collection"]]
    
    pipeline = [
        {"$group": {"_id": "$day", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    result = list(collection.aggregate(pipeline))
    client.close()
    return {str(r["_id"]): r["count"] for r in result}


def get_starrocks_count_by_day():
    """按day分组统计StarRocks"""
    conn = pymysql.connect(
        host=STARROCKS_CONFIG["host"],
        port=STARROCKS_CONFIG["port"],
        user=STARROCKS_CONFIG["user"],
        password=STARROCKS_CONFIG["password"],
        database=STARROCKS_CONFIG["db"],
        connect_timeout=30
    )
    cursor = conn.cursor()
    sql = f"SELECT day, COUNT(*) FROM {STARROCKS_CONFIG['db']}.{STARROCKS_CONFIG['table']} GROUP BY day ORDER BY day"
    cursor.execute(sql)
    result = cursor.fetchall()
    conn.close()
    return {str(r[0]): r[1] for r in result}


if __name__ == "__main__":
    print("数据量对比工具 - MongoDB vs StarRocks")
    print("对比时间: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    print("=" * 60)
    
    try:
        mongo_total = get_mongodb_count()
        sr_total = get_starrocks_count()
        
        print("\n>>> 总数据量对比:")
        print(f"MongoDB: {mongo_total:,} 条")
        print(f"StarRocks: {sr_total:,} 条")
        print(f"差异: {abs(mongo_total - sr_total):,} 条")
        
        if mongo_total == sr_total:
            print("\n✅ 数据量完全对齐！")
        else:
            print("\n⚠️  数据量不对齐，按天分组对比...")
            mongo_by_day = get_mongodb_count_by_day()
            sr_by_day = get_starrocks_count_by_day()
            
            print("\n>>> 按天分组对比:")
            print(f"{'Day':<10} {'MongoDB':>10} {'StarRocks':>10} {'差异':>10}")
            print("-" * 45)
            
            all_days = set(mongo_by_day.keys()).union(set(sr_by_day.keys()))
            mismatch = 0
            for day in sorted(all_days):
                m_count = mongo_by_day.get(day, 0)
                s_count = sr_by_day.get(day, 0)
                diff = m_count - s_count
                if diff != 0:
                    mismatch += 1
                    print(f"{day:<10} {m_count:>10,} {s_count:>10,} {diff:>+10,}")
            
            if mismatch == 0:
                print("\n✅ 按天统计完全对齐！")
            else:
                print(f"\n❌ 发现 {mismatch} 天数据量不对齐")
    
    except Exception as e:
        print(f"\n❌ 对比过程出错: {str(e)}")
        import traceback
        traceback.print_exc()

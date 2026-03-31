#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymongo
from pymongo import MongoClient
import pymysql
import json
from datetime import datetime
from bson import ObjectId, Decimal128, Int64

# 配置
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

CHECKPOINT_FILE = "./sync_checkpoint.json"

def main():
    print("=" * 60)
    print("开始对比MongoDB与StarRocks整体数据差异")
    print("=" * 60)
    
    # 读取最新断点
    with open(CHECKPOINT_FILE, 'r') as f:
        checkpoint = json.load(f)
    last_updated_time = checkpoint.get('last_updated_time')
    print("最新断点时间: %s" % last_updated_time)
    
    # 连接MongoDB统计文档数
    print("\n1. 统计MongoDB数据...")
    mongo_client = MongoClient(
        host=MONGO_CONFIG['host'],
        port=MONGO_CONFIG['port'],
        username=MONGO_CONFIG['username'],
        password=MONGO_CONFIG['password'],
        authSource=MONGO_CONFIG['auth_db']
    )
    mongo_db = mongo_client[MONGO_CONFIG['db']]
    mongo_collection = mongo_db[MONGO_CONFIG['collection']]
    
    # 统计全表
    total_mongo_count = mongo_collection.count_documents({})
    print("   MongoDB总文档数: %d" % total_mongo_count)
    
    # 连接StarRocks统计行数
    print("\n2. 统计StarRocks数据...")
    conn = pymysql.connect(
        host=STARROCKS_CONFIG['host'],
        port=STARROCKS_CONFIG['port'],
        user=STARROCKS_CONFIG['user'],
        password=STARROCKS_CONFIG['password'],
        database=STARROCKS_CONFIG['db'],
        connect_timeout=10
    )
    
    cursor = conn.cursor()
    # 统计全表行数
    cursor.execute("SELECT COUNT(*) as count FROM %s" % STARROCKS_CONFIG['table'])
    result = cursor.fetchone()
    total_sr_count = result[0] if result else 0
    print("   StarRocks总行数: %d" % total_sr_count)
    
    # 计算差异
    total_diff = total_mongo_count - total_sr_count
    print("\n" + "=" * 60)
    print("整体对比结果:")
    print("=" * 60)
    print("MongoDB总文档数: %d" % total_mongo_count)
    print("StarRocks总行数: %d" % total_sr_count)
    print("差异 (MongoDB - StarRocks): %d" % total_diff)
    
    # 同时对比增量范围
    print("\n" + "=" * 60)
    print("增量范围再次验证 (updatedTime >= %s):" % last_updated_time)
    print("=" * 60)
    
    dt = datetime.strptime(last_updated_time, "%Y-%m-%d %H:%M:%S.%f")
    inc_mongo_count = mongo_collection.count_documents({"updatedTime": {"$gte": dt}})
    sql = "SELECT COUNT(*) as count FROM %s WHERE updatedTime >= '%s'" % (STARROCKS_CONFIG['table'], last_updated_time)
    cursor.execute(sql)
    result = cursor.fetchone()
    inc_sr_count = result[0] if result else 0
    inc_diff = inc_mongo_count - inc_sr_count
    
    print("增量MongoDB: %d" % inc_mongo_count)
    print("增量StarRocks: %d" % inc_sr_count)
    print("增量差异: %d" % inc_diff)
    
    print("\n" + "=" * 60)
    if total_diff == 0:
        print("✅ 整体数据完全一致！")
    else:
        print("⚠️ 整体数据存在差异，请检查同步历史")
    
    cursor.close()
    conn.close()
    mongo_client.close()

if __name__ == "__main__":
    main()

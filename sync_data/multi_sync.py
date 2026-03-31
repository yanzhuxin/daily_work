#!/usr/bin/env python3
import multiprocessing
import pymongo
from pymongo import MongoClient
from bson import ObjectId
import subprocess
import os
import json

MONGO_CONFIG = {
    "host": "10.34.137.87",
    "port": 37018,
    "username": "nodeDayOpsWide_r",
    "password": "pRkuawfIRKXkRQu1nhTLYhjF96QpAyXXYou",
    "auth_db": "jarvis",
    "db": "jarvis",
    "collection": "nodeDayOpsWide",
}

def get_id_ranges(num_shards=4):
    """获取MongoDB集合的ID分片范围"""
    client = MongoClient(
        host=MONGO_CONFIG["host"],
        port=MONGO_CONFIG["port"],
        username=MONGO_CONFIG["username"],
        password=MONGO_CONFIG["password"],
        authSource=MONGO_CONFIG["auth_db"],
    )
    coll = client[MONGO_CONFIG["db"]][MONGO_CONFIG["collection"]]
    
    # 获取最小和最大ID
    min_id = coll.find().sort("_id", 1).limit(1)[0]["_id"]
    max_id = coll.find().sort("_id", -1).limit(1)[0]["_id"]
    
    # 转换为16进制整数分片
    min_int = int(str(min_id), 16)
    max_int = int(str(max_id), 16)
    step = (max_int - min_int) // num_shards
    
    ranges = []
    for i in range(num_shards):
        start_int = min_int + i * step
        end_int = min_int + (i+1) * step if i < num_shards-1 else max_int + 1
        start_id = ObjectId(hex(start_int)[2:].zfill(24))
        end_id = ObjectId(hex(end_int)[2:].zfill(24))
        ranges.append((start_id, end_id, i))
    
    client.close()
    return ranges

def sync_shard(start_id, end_id, shard_id):
    """同步单个分片"""
    print(f"启动分片 {shard_id} 同步，ID范围: {start_id} ~ {end_id}")
    
    # 每个分片独立断点
    checkpoint_file = f"./sync_checkpoint_shard_{shard_id}.json"
    os.environ["CHECKPOINT_FILE"] = checkpoint_file
    
    # 生成独立的同步脚本
    with open("mongodb2starRocks.py", "r", encoding="utf-8") as f:
        content = f.read()
    
    # 注入分片查询条件
    inject_code = f'''
# 分片查询条件注入
query["_id"] = {{"$gte": ObjectId("{start_id}"), "$lt": ObjectId("{end_id}")}}
'''
    content = content.replace('    query = {}', f'    query = {inject_code}')
    
    # 保存分片脚本
    shard_script = f"./sync_shard_{shard_id}.py"
    with open(shard_script, "w", encoding="utf-8") as f:
        f.write(content)
    
    # 执行同步
    subprocess.run(["python3", shard_script, "full"], check=True)
    print(f"分片 {shard_id} 同步完成")

if __name__ == "__main__":
    num_shards = 4
    print(f"初始化 {num_shards} 个并行同步分片...")
    id_ranges = get_id_ranges(num_shards)
    
    # 启动多进程
    processes = []
    for start_id, end_id, shard_id in id_ranges:
        p = multiprocessing.Process(target=sync_shard, args=(start_id, end_id, shard_id))
        p.start()
        processes.append(p)
    
    # 等待所有进程完成
    for p in processes:
        p.join()
    
    print("所有分片同步完成！")
    
    # 汇总统计
    import pymysql
    conn = pymysql.connect(
        host="10.70.33.22",
        port=9030,
        user="srtest",
        password="srtest@890",
        database="test"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM node_day_ops_wide")
    cnt = cursor.fetchone()[0]
    print(f"总同步数据量: {cnt:,} 条")
    print(f"目标数据量: 2,667,005 条")
    print(f"同步结果: {'✅ 完全一致' if cnt == 2667005 else '❌ 数据不一致'}")
    cursor.close()
    conn.close()

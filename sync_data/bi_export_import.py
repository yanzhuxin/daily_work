#!/usr/bin/env python3
import pandas as pd
import pymysql
import os

# 连接MongoDB BI Connector
mysql_conn = pymysql.connect(
    host="127.0.0.1",
    port=3307,
    user="nodeDayOpsWide_r",
    password="pRkuawfIRKXkRQu1nhTLYhjF96QpAyXXYou",
    database="jarvis",
    charset="utf8mb4"
)

print("开始从BI Connector读取数据...")
# 分批读取，避免内存溢出
chunk_size = 50000
offset = 0
total = 0
csv_file = "mongo_bi_export.csv"

# 如果文件存在先删除
if os.path.exists(csv_file):
    os.remove(csv_file)

while True:
    sql = f"SELECT * FROM nodeDayOpsWide LIMIT {chunk_size} OFFSET {offset}"
    df = pd.read_sql(sql, mysql_conn)
    if len(df) == 0:
        break
    
    # 写入CSV，首行只写一次
    df.to_csv(csv_file, mode="a", header=(offset==0), index=False, encoding="utf-8")
    total += len(df)
    offset += chunk_size
    print(f"已导出 {total} 条数据")

print(f"导出完成，共 {total} 条数据，保存到 {csv_file}")
mysql_conn.close()

# 开始导入到StarRocks
print("\n开始导入到StarRocks...")
import subprocess
cmd = f"""curl --location-trusted -u srtest:srtest@890 -T {csv_file} \
http://10.70.33.22:8030/api/test/node_day_ops_wide/_stream_load \
-H "format: csv" \
-H "column_separator: ," \
-H "skip_header: 1" \
-H "columns: customerId,day,nodeId,analyzePeak95,baseInfo_channelId,baseInfo_signatoryId,baseInfo_bandwidth,buildBandwidth,city,cost_guaranteedRate,cost_priceItemId,cost_priceItemName,cost_priceType,cost_price,cost_priceAfterBonus,cost_measure,cost_original,cost_bonus,cost_slaDeduction,cost_tobaDeduction,cost_settlement,cost_adjustmentAmount,cost_finalAmount,customerName,deliveryType,evening20To23Avg,eveningAvg,eveningPeak95,isBanTransProv,isp,name,natType,nodeTags,nodeType,os,peak95,peak95Ratio,peak95Time,peakMaxRatio,priceNumber,profit_profitAmount,profit_profitRate,profit_estimatedProfitAmount,profit_estimatedProfitRate,province,purchaserName,quantityEnd,quantityType,realISP,resourceType,revenue_guaranteedRate,revenue_priceItemId,revenue_priceItemName,revenue_price,revenue_measure,revenue_coefficientMeasure,revenue_amount,revenue_finalAmount,revenue_estimatedFinalAmount,signatoryName,snapshotTime,stage,stairType,stairs,state,tcpNatType,udpNatType,unEveningAvg,updatedTime,vendorId,vendorSuggestCustomers,virtualCustomers,webPort,webPortResult"
"""
result = subprocess.getoutput(cmd)
print("导入结果:")
print(result)

# 验证数据量
print("\n验证数据一致性:")
import pymysql
sr_conn = pymysql.connect(
    host="10.70.33.22",
    port=9030,
    user="srtest",
    password="srtest@890",
    database="test"
)
cursor = sr_conn.cursor()
cursor.execute("SELECT COUNT(*) FROM node_day_ops_wide")
sr_cnt = cursor.fetchone()[0]
print(f"StarRocks数据量: {sr_cnt:,}")
print(f"MongoDB数据量: 2,667,005")
print(f"同步一致: {sr_cnt == 2667005}")
cursor.close()
sr_conn.close()

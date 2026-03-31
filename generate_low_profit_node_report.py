#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymysql
import pandas as pd
from datetime import datetime
import os

# StarRocks配置
STARROCKS_CONFIG = {
    "host": "10.70.33.22",
    "port": 9030,
    "user": "srtest",
    "password": "srtest@890",
    "db": "test",
}

# 判定参数配置
CONFIG = {
    "stats_days": 30,       # 统计最近多少天
    "min_online_days": 7,   # 最小在线天数
    "max_negative_ratio": 50,  # 最大负毛利天数占比
    "max_profit_rate": 5,   # 最大允许平均毛利率(%)
}

def main():
    # 连接数据库
    conn = pymysql.connect(**STARROCKS_CONFIG)
    
    # 查询低收益节点
    sql = f"""
    SELECT 
        nodeId,
        customerId,
        customerName,
        vendorId,
        province,
        city,
        nodeType,
        days_online,
        total_cost,
        total_revenue,
        total_profit,
        avg_profit_rate,
        negative_days,
        negative_days_ratio,
        start_date,
        end_date
    FROM node_30day_profit
    WHERE days_online >= {CONFIG['min_online_days']}
      AND (
          total_profit < 0 
          OR avg_profit_rate < {CONFIG['max_profit_rate']}
          OR negative_days_ratio > {CONFIG['max_negative_ratio']}
      )
    ORDER BY total_profit ASC;
    """
    
    print(f"正在查询最近 {CONFIG['stats_days']} 天的数据...")
    df = pd.read_sql(sql, conn)
    conn.close()
    
    # 生成报告
    today = datetime.now().strftime('%Y%m%d')
    report_dir = '/home/yanzhuxin/daily_work/reports'
    os.makedirs(report_dir, exist_ok=True)
    report_file = f'{report_dir}/low_profit_nodes_{today}.xlsx'
    
    df.to_excel(report_file, index=False)
    
    # 控制台输出
    print(f"\n=== 低收益节点分析报告 ===")
    print(f"报告日期: {today}")
    print(f"统计范围: 最近 {CONFIG['stats_days']} 天")
    print(f"发现低收益节点: {len(df)} 个")
    if len(df) > 0:
        print(f"累计亏损总额: {df['total_profit'].sum():.2f}")
    print(f"报告已保存到: {report_file}")

if __name__ == "__main__":
    main()
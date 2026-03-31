#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymysql
import pandas as pd
from datetime import datetime
import os

STARROCKS_CONFIG = {
    "host": "10.70.33.22",
    "port": 9030,
    "user": "srtest",
    "password": "srtest@890",
    "db": "test",
}

def main():
    conn = pymysql.connect(**STARROCKS_CONFIG)
    today = datetime.now().strftime('%Y%m%d')
    
    # 1. 供应商分析
    print("生成供应商分析报告...")
    vendor_df = pd.read_sql("SELECT * FROM vendor_profit_analysis ORDER BY 总毛利 ASC", conn)
    
    # 2. 机房分析
    print("生成机房分析报告...")
    idc_df = pd.read_sql("SELECT * FROM idc_profit_analysis ORDER BY 总毛利 ASC", conn)
    
    # 3. 客户分析
    print("生成客户业务分析报告...")
    customer_df = pd.read_sql("SELECT * FROM customer_profit_analysis ORDER BY 总毛利 ASC", conn)
    
    conn.close()
    
    # 确保报告目录存在
    report_dir = '/home/yanzhuxin/daily_work/reports'
    os.makedirs(report_dir, exist_ok=True)
    
    # 保存到Excel
    report_file = f'{report_dir}/profit_dimension_analysis_{today}.xlsx'
    
    with pd.ExcelWriter(report_file) as writer:
        vendor_df.to_excel(writer, sheet_name='1-供应商维度分析', index=False)
        idc_df.to_excel(writer, sheet_name='2-机房维度分析', index=False)
        customer_df.to_excel(writer, sheet_name='3-客户业务维度分析', index=False)
    
    # 控制台输出发现的TOP问题
    print("\n=== 分析发现 ===")
    
    if len(vendor_df) > 0:
        print(f"\n供应商亏损TOP3:")
        for _, row in vendor_df.head(3).iterrows():
            print(f"  供应商ID: {row['vendorId']}, 亏损节点: {row['亏损节点数']}/{row['总节点数']} ({row['亏损节点占比_pct']}%), 总毛利: {row['总毛利']:.2f}")
    
    if len(idc_df) > 0:
        print(f"\n机房亏损TOP3:")
        for _, row in idc_df.head(3).iterrows():
            print(f"  IDC ID: {row['idcId']} {row['province']}{row['city']}, 亏损节点: {row['亏损节点数']}/{row['总节点数']} ({row['亏损节点占比_pct']}%), 总毛利: {row['总毛利']:.2f}")
    
    if len(customer_df) > 0:
        print(f"\n客户亏损TOP3:")
        for _, row in customer_df.head(3).iterrows():
            print(f"  客户: {row['customerName']}, 亏损节点: {row['亏损节点数']}/{row['总节点数']} ({row['亏损节点占比_pct']}%), 总毛利: {row['总毛利']:.2f}")
    
    print(f"\n完整报告已保存: {report_file}")

if __name__ == "__main__":
    main()
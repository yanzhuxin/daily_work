#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymysql
import pandas as pd
from datetime import datetime, timedelta
import os

STARROCKS_CONFIG = {
    "host": "10.70.33.22",
    "port": 9030,
    "user": "srtest",
    "password": "srtest@890",
    "db": "test",
}

def calculate_optimization_effect(conn, dimension, target_id, action_date):
    """计算单个优化动作的效果"""
    
    # 优化前30天
    before_start = (action_date - timedelta(days=30)).strftime('%Y-%m-%d')
    before_end = action_date.strftime('%Y-%m-%d')
    
    # 根据维度拼接where条件
    if dimension == 'vendor':
        where = "n.vendorId = '%s'" % target_id
    elif dimension == 'idc':
        where = "n.idcId = '%s'" % target_id
    elif dimension == 'customer':
        where = "n.customerId = '%s'" % target_id
    else:
        where = "1=1"
    
    sql_before = f"""
    SELECT SUM(p.total_profit) as total_profit
    FROM test.node_30day_profit p
    JOIN test.node_day_ops_wide_full n ON p.nodeId = n.nodeId
    WHERE {where} AND n.day BETWEEN '{before_start}' AND '{before_end}'
    """
    
    before_df = pd.read_sql(sql_before, conn)
    before_profit = before_df['total_profit'][0] if len(before_df) > 0 and before_df['total_profit'][0] is not None else 0
    
    return before_profit

def calculate_after_effect(conn, dimension, target_id, action_date):
    """计算优化后效果"""
    
    # 计算已经过去多少天
    now = datetime.now()
    days_passed = (now - action_date).days
    
    # 优化后到目前的数据
    after_start = action_date.strftime('%Y-%m-%d')
    after_end = now.strftime('%Y-%m-%d')
    
    # 根据维度拼接where条件
    if dimension == 'vendor':
        where = "n.vendorId = '%s'" % target_id
    elif dimension == 'idc':
        where = "n.idcId = '%s'" % target_id
    elif dimension == 'customer':
        where = "n.customerId = '%s'" % target_id
    else:
        where = "1=1"
    
    sql_after = f"""
    SELECT SUM(p.total_profit) as total_profit
    FROM test.node_30day_profit p
    JOIN test.node_day_ops_wide_full n ON p.nodeId = n.nodeId
    WHERE {where} AND n.day BETWEEN '{after_start}' AND '{after_end}'
    """
    
    after_df = pd.read_sql(sql_after, conn)
    after_profit = after_df['total_profit'][0] if len(after_df) > 0 and after_df['total_profit'][0] is not None else 0
    
    return after_profit, days_passed

def generate_optimization_effect_report():
    """生成所有已完成优化动作的效果报告"""
    conn = pymysql.connect(**STARROCKS_CONFIG)
    
    # 查询所有已完成的动作
    sql = "SELECT * FROM profit_optimization_actions WHERE status = 'done' ORDER BY action_date DESC"
    actions_df = pd.read_sql(sql, conn)
    
    results = []
    for _, row in actions_df.iterrows():
        dimension = row['dimension']
        target_id = row['target_id']
        action_date = row['action_date']
        
        if not isinstance(action_date, datetime):
            action_date = datetime.combine(action_date, datetime.min.time())
        
        before_profit = row['before_total_profit']
        after_profit, days_passed = calculate_after_effect(conn, dimension, target_id, action_date)
        
        # 年化换算到30天对比
        if days_passed > 0:
            after_profit_30d = after_profit / days_passed * 30
        else:
            after_profit_30d = after_profit
        
        profit_increase = after_profit_30d - before_profit
        
        results.append({
            'action_id': row['action_id'],
            'action_date': row['action_date'],
            'dimension': dimension,
            'target_id': target_id,
            'action_type': row['action_type'],
            'before_30d_profit': round(before_profit, 2),
            'after_30d_profit': round(after_profit_30d, 2),
            'profit_increase_30d': round(profit_increase, 2),
            'status': row['status']
        })
    
    result_df = pd.DataFrame(results)
    
    # 生成报告
    today = datetime.now().strftime('%Y%m%d')
    report_dir = '/home/yanzhuxin/daily_work/reports'
    os.makedirs(report_dir, exist_ok=True)
    report_file = f'{report_dir}/optimization_effect_{today}.xlsx'
    
    result_df.to_excel(report_file, index=False)
    
    conn.close()
    
    # 输出汇总
    total_increase = result_df['profit_increase_30d'].sum()
    print(f"\n=== 优化效果汇总报告 ===")
    print(f"报告日期: {today}")
    print(f"已完成优化动作: {len(result_df)} 个")
    print(f"总体月毛利提升: {total_increase:.2f}")
    print(f"报告已保存: {report_file}")
    
    return result_df

if __name__ == "__main__":
    generate_optimization_effect_report()
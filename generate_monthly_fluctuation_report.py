#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
月维度前后两日波动分析报告生成脚本
功能：获取过去12个月数据，对比昨天和今天同一月份的汇总数据，分析月度波动情况，推送到企业微信测试版
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import requests
import markdown
import os

sys.path.insert(0, "/Volumes/system/pypro")
from guandata_client import GuanDataFetcher, FilterCondition, get_token

def load_and_preprocess_data(df):
    """加载并预处理数据"""
    # 数值列转换为浮点型
    numeric_cols = ["计费金额", "成本金额", "毛利_new", "成本带宽G", "计费带宽G"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 处理统计日期，提取日期部分
    df["统计日期"] = pd.to_datetime(df["统计日期"]).dt.date
    # 统计日期-1天，还原数据真实时间
    df["统计日期"] = df["统计日期"] - pd.Timedelta(days=1)
    # 提取月份
    df["月份"] = df["统计日期"].apply(lambda x: x.strftime("%Y-%m"))
    return df


def get_compare_days(df):
    """获取最新两天（昨天和今天）"""
    dates = sorted(df["统计日期"].unique())
    if len(dates) < 2:
        return None, None
    latest_day = dates[-1]
    prev_day = dates[-2]
    return latest_day, prev_day


def calculate_monthly_metrics(df, latest_day, prev_day):
    """按月份计算两天的指标对比"""
    # 分别汇总两天每个月的数据
    latest_agg = df[df["统计日期"] == latest_day].groupby("月份")[
        ["计费金额", "成本金额", "毛利_new", "计费带宽G", "成本带宽G"]
    ].sum().reset_index()

    prev_agg = df[df["统计日期"] == prev_day].groupby("月份")[
        ["计费金额", "成本金额", "毛利_new", "计费带宽G", "成本带宽G"]
    ].sum().reset_index()

    # 合并数据
    merged = pd.merge(
        prev_agg, latest_agg, on="月份", how="outer", suffixes=("_prev", "_latest")
    ).fillna(0)

    # 计算变化
    merged["计费金额_diff"] = merged["计费金额_latest"] - merged["计费金额_prev"]
    merged["计费金额_rate"] = merged.apply(
        lambda x: (
            x["计费金额_diff"] / x["计费金额_prev"] * 100
            if x["计费金额_prev"] != 0
            else (np.inf if x["计费金额_diff"] > 0 else -np.inf)
        ),
        axis=1,
    )

    merged["成本金额_diff"] = merged["成本金额_latest"] - merged["成本金额_prev"]
    merged["成本金额_rate"] = merged.apply(
        lambda x: (
            x["成本金额_diff"] / x["成本金额_prev"] * 100
            if x["成本金额_prev"] != 0
            else (np.inf if x["成本金额_diff"] > 0 else -np.inf)
        ),
        axis=1,
    )

    merged["毛利_diff"] = merged["毛利_new_latest"] - merged["毛利_new_prev"]
    merged["毛利_rate"] = merged.apply(
        lambda x: (
            x["毛利_diff"] / x["毛利_new_prev"] * 100
            if x["毛利_new_prev"] != 0
            else (np.inf if x["毛利_diff"] > 0 else -np.inf)
        ),
        axis=1,
    )

    merged["计费带宽_diff"] = merged["计费带宽G_latest"] - merged["计费带宽G_prev"]
    merged["成本带宽_diff"] = merged["成本带宽G_latest"] - merged["成本带宽G_prev"]

    # 获取过去12个月
    current_date = datetime.now()
    months = []
    for i in range(12):
        month = (current_date - timedelta(days=30*i)).strftime("%Y-%m")
        months.append(month)
    
    # 只保留过去12个月，并按毛利变化绝对值降序排序
    merged = merged[merged["月份"].isin(months)]
    merged["毛利_diff_abs"] = abs(merged["毛利_diff"])
    merged = merged.sort_values("毛利_diff_abs", ascending=False).reset_index(drop=True)

    return merged

def generate_markdown_report(monthly_result, latest_day, prev_day, output_path):
    """生成Markdown格式的分析报告"""
    report_content = """# 月维度前后两日波动分析报告（排除七牛CDN）
生成时间: {datetime_now}
对比日期: {prev_day} (昨日) → {latest_day} (今日)
筛选条件: 排除七牛CDN，只展示过去12个月数据

---

## 一、整体汇总对比
| 指标       | 昨日汇总       | 今日汇总       | 变化金额       | 变化率      |
|------------|---------------|---------------|----------------|-------------|
""".format(datetime_now=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
           prev_day=prev_day, 
           latest_day=latest_day)

    # 整体汇总
    total_prev_cost = monthly_result["成本金额_prev"].sum()
    total_latest_cost = monthly_result["成本金额_latest"].sum()
    total_cost_diff = total_latest_cost - total_prev_cost
    total_cost_rate = (total_cost_diff / total_prev_cost * 100) if total_prev_cost !=0 else np.inf

    total_prev_revenue = monthly_result["计费金额_prev"].sum()
    total_latest_revenue = monthly_result["计费金额_latest"].sum()
    total_revenue_diff = total_latest_revenue - total_prev_revenue
    total_revenue_rate = (total_revenue_diff / total_prev_revenue * 100) if total_prev_revenue !=0 else np.inf

    total_prev_profit = monthly_result["毛利_new_prev"].sum()
    total_latest_profit = monthly_result["毛利_new_latest"].sum()
    total_profit_diff = total_latest_profit - total_prev_profit
    total_profit_rate = (total_profit_diff / total_prev_profit * 100) if total_prev_profit !=0 else np.inf

    total_prev_bw = monthly_result["计费带宽G_prev"].sum()
    total_latest_bw = monthly_result["计费带宽G_latest"].sum()
    total_bw_diff = total_latest_bw - total_prev_bw

    # 添加整体表格
    report_content += "| 成本金额   | {total_prev_cost:,.2f} | {total_latest_cost:,.2f} | {total_cost_diff:+,.2f} | {total_cost_rate:,.2f}% |\n".format(**locals())
    report_content += "| 计费金额   | {total_prev_revenue:,.2f} | {total_latest_revenue:,.2f} | {total_revenue_diff:+,.2f} | {total_revenue_rate:,.2f}% |\n".format(**locals())
    report_content += "| 毛利       | {total_prev_profit:,.2f} | {total_latest_profit:,.2f} | {total_profit_diff:+,.2f} | {total_profit_rate:,.2f}% |\n".format(**locals())
    report_content += "| 计费带宽   | {total_prev_bw:,.2f}G | {total_latest_bw:,.2f}G | {total_bw_diff:+,.2f}G | - |\n".format(**locals())

    report_content += """
---

## 二、各月份波动明细（按毛利变化绝对值排序）
| 月份    | 昨日毛利   | 今日毛利   | 毛利变化   | 变化率   | 成本变化   | 成本变化率 | 收入变化   | 收入变化率 | 计费带宽变化 |
|---------|------------|------------|------------|----------|------------|------------|------------|------------|--------------|
"""

    for _, row in monthly_result.iterrows():
        month = row["月份"]
        profit_prev = row["毛利_new_prev"]
        profit_latest = row["毛利_new_latest"]
        profit_diff = row["毛利_diff"]
        profit_rate = row["毛利_rate"]

        cost_diff = row["成本金额_diff"]
        cost_rate = row["成本金额_rate"]
        revenue_diff = row["计费金额_diff"]
        revenue_rate = row["计费金额_rate"]
        bw_diff = row["计费带宽_diff"]

        icon = "↑" if profit_diff > 0 else "↓"
        color = "red" if profit_diff > 0 else "green"

        report_content += "| {month} | {profit_prev:,.2f} | {profit_latest:,.2f} | {icon} <font color='{color}'>{profit_diff:+,.2f}</font> | {profit_rate:+,.2f}% | {cost_diff:+,.2f} | {cost_rate:+,.2f}% | {revenue_diff:+,.2f} | {revenue_rate:+,.2f}% | {bw_diff:+,.2f}G |\n".format(**locals())

    growth_profit = "增长" if total_profit_diff > 0 else "下降"
    growth_revenue = "增长" if total_revenue_diff > 0 else "下降"
    growth_cost = "增长" if total_cost_diff > 0 else "下降"
    
    extra_line = ""
    if len(monthly_result) > 0:
        diff = monthly_result.iloc[0]['毛利_diff']
        month = monthly_result.iloc[0]['月份']
        if diff > 0:
            extra_line = "4. 最大波动月份：**%s**，毛利变化+%.2f" % (month, diff)
        else:
            extra_line = "4. 最大波动月份：**%s**，毛利变化%.2f" % (month, diff)
    
    report_content += """
---

## 三、核心结论
1. 整体毛利环比**{growth_profit}** {total_profit_diff:+,.2f}，增幅{total_profit_rate:+,.2f}%
2. 整体收入{growth_revenue} {total_revenue_diff:+,.2f}，增幅{total_revenue_rate:+,.2f}%
3. 整体成本{growth_cost} {total_cost_diff:+,.2f}，增幅{total_cost_rate:+,.2f}%
{extra_line}
""".format(**locals())

    # 写入文件
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report_content)

    print("报告已生成: {output_path}".format(**locals()))
    return report_content

def extract_core_conclusion(markdown_content):
    """提取核心结论部分"""
    lines = markdown_content.split('\n')
    core_conclusion = []
    in_core = False

    for line in lines:
        if "## 一、整体汇总对比" in line:
            in_core = True
        elif "---" in line and in_core:
            break
        elif in_core:
            core_conclusion.append(line)

    # 添加核心结论部分
    found_core = False
    for line in lines:
        if "## 三、核心结论" in line:
            found_core = True
        if found_core:
            core_conclusion.append(line)
            if line.strip() == "":
                break

    return '\n'.join(core_conclusion)


def send_to_wechat_webhook(webhook_url, markdown_content, report_path):
    """推送Markdown报告到企业微信机器人，超长自动推送核心结论+附件文件"""
    headers = {"Content-Type": "application/json"}
    MAX_MARKDOWN_LEN = 4000  # 企业微信markdown最大长度限制4096字节，留余量
    webhook_key = webhook_url.split("key=")[-1]

    # 判断内容长度（按字节数计算，中文占3字节）
    if len(markdown_content.encode("utf-8")) <= MAX_MARKDOWN_LEN:
        # 内容不超长，直接推送全文
        payload = {"msgtype": "markdown", "markdown": {"content": markdown_content}}
        push_type = "全文"
        try:
            response = requests.post(
                webhook_url, json=payload, headers=headers, timeout=10
            )
            response_data = response.json()
            if response_data.get("errcode") == 0:
                print("✅ 报告已成功推送至企业微信（{push_type}）")
                return True
            else:
                print(
                    "❌ 推送失败，错误信息：{response_data.get('errmsg', '未知错误')}".format(**locals())
                )
                return False
        except Exception as e:
            print("❌ 推送异常：{str(e)}")
            return False
    else:
        # 内容超长：1. 上传文件到企业微信临时素材 2. 推送核心结论+文件附件
        print("ℹ️  报告内容超长，将推送核心结论+完整报告附件")
        try:
            # 第一步：将markdown转为带样式的HTML文件
            html_template = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>月维度前后两日波动分析报告</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; padding: 20px; max-width: 1200px; margin: 0 auto; }
        h1, h2 { color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }
        table { border-collapse: collapse; width: 100%; margin: 15px 0; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f8f9fa; }
        font[color="red"] { color: #e74c3c !important; font-weight: bold; }
        font[color="green"] { color: #27ae60 !important; font-weight: bold; }
    </style>
</head>
<body>
{}
</body>
</html>
            """
            # markdown转html，支持表格等扩展
            html_body = markdown.markdown(
                markdown_content, extensions=["tables", "extra"]
            )
            html_content = html_template.format(html_body)
            # 保存HTML文件
            html_path = report_path.replace(".md", ".html")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html_content)

            # 第二步：上传HTML文件到企业微信临时素材
            upload_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key={webhook_key}&type=file".format(**locals())
            files = {"media": open(html_path, "rb")}
            upload_resp = requests.post(upload_url, files=files, timeout=15)
            upload_data = upload_resp.json()
            if upload_data.get("errcode") != 0:
                print("❌ 文件上传失败：{upload_data.get('errmsg', '未知错误')}")
                os.remove(html_path)  # 清理临时文件
                return False
            media_id = upload_data["media_id"]
            os.remove(html_path)  # 上传成功后清理临时HTML文件

            # 第二步：推送核心结论
            overall_part = ""
            conclusion_part = ""
            sections = markdown_content.split("---")
            if len(sections) >= 1:
                overall_part = sections[0].strip() + "\n\n"
            if len(sections) >= 3:
                conclusion_part = sections[-1].strip()

            datetime_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            push_content = """# 月维度前后两日波动分析报告（超长精简版）
生成时间: {datetime_now}
{overall_part}
{conclusion_part}

> 📎 完整报告见下方附件
 """.format(**locals())
            # 先推核心结论
            payload = {"msgtype": "markdown", "markdown": {"content": push_content}}
            resp1 = requests.post(
                webhook_url, json=payload, headers=headers, timeout=10
            )

            # 再推文件附件
            payload_file = {"msgtype": "file", "file": {"media_id": media_id}}
            resp2 = requests.post(
                webhook_url, json=payload_file, headers=headers, timeout=10
            )

            if resp1.json().get("errcode") == 0 and resp2.json().get("errcode") == 0:
                print("✅ 报告已成功推送至企业微信（核心结论+完整附件）".format(**locals()))
                return True
            else:
                print(
                    "❌ 推送失败：{resp1.json().get('errmsg', '')} {resp2.json().get('errmsg', '')}".format(**locals())
                )
                return False

        except Exception as e:
            print("❌ 推送异常：{str(e)}")
            return False

if __name__ == "__main__":
    # 配置
    OUTPUT_DIR = "/home/yanzhuxin/guany/reports/"
    OUTPUT_REPORT_PATH = OUTPUT_DIR + "月维度前后两日波动分析报告_排除七牛CDN.md"
    # 企业微信推送配置：使用测试版
    ENABLE_WECHAT_PUSH = True
    WECHAT_WEBHOOK = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=36887f02-3fcf-46ac-a13f-69ddf0ddb595"
    USE_TEST_WEBHOOK = True  # 使用测试webhook

    # 1. 获取数据
    print("===== 开始获取数据 =====")
    # 当前月份动态筛选：当月3号及以后选本月，否则选上月
    today = datetime.now()
    if today.day >= 3:
        filter_month = today.strftime("%Y-%m")
    else:
        # 计算上月
        last_month = today.replace(day=1) - timedelta(days=1)
        filter_month = last_month.strftime("%Y-%m")
    
    print("筛选月份: {filter_month}")
    
    fc = FilterCondition()
    fc.eq("合并月份", filter_month).ne("客户_new", "七牛CDN")
    token = get_token()
    result2 = GuanDataFetcher.fetch_data(
        token=token, ds_id="eff95e2a2fe0048dfb9727b1", filter_condition=fc, limit=50000
    )
    col = result2.get("columns", [])
    colnames = [col[i]["name"] for i in range(len(col))]
    df2 = pd.DataFrame(result2.get("preview", []), columns=colnames)
    print("获取数据完成，共 {len(df2)} 行")

    # 2. 数据预处理
    print("\n===== 开始数据预处理 =====")
    df = load_and_preprocess_data(df2)
    print("预处理完成")

    # 3. 获取对比日期
    latest_day, prev_day = get_compare_days(df)
    if latest_day is None or prev_day is None:
        print("❌ 数据不足两天，无法分析")
        exit(1)
    print("分析日期: {prev_day} (昨日) → {latest_day} (今日)")

    # 4. 计算月度指标对比
    print("\n===== 开始计算月度指标对比 =====")
    monthly_result = calculate_monthly_metrics(df, latest_day, prev_day)
    print("计算完成，共 {len(monthly_result)} 个月份符合条件")

    # 5. 生成分析报告
    print("\n===== 生成分析报告 =====")
    report = generate_markdown_report(monthly_result, latest_day, prev_day, OUTPUT_REPORT_PATH)

    # 6. 推送至企业微信机器人（测试版）
    if ENABLE_WECHAT_PUSH:
        print("\n正在推送报告到企业微信测试版...")
        send_to_wechat_webhook(WECHAT_WEBHOOK, report, OUTPUT_REPORT_PATH)

    # 打印核心结论
    print("\n===== 核心结论 =====")
    total_prev_profit = monthly_result["毛利_new_prev"].sum()
    total_latest_profit = monthly_result["毛利_new_latest"].sum()
    total_profit_diff = total_latest_profit - total_prev_profit
    total_profit_rate = (total_profit_diff / total_prev_profit * 100) if total_prev_profit !=0 else np.inf
    print(
        "总毛利变化: %+.2f (%+.2f%%)" % (total_profit_diff, total_profit_rate)
    )
    if len(monthly_result) > 0:
        month = monthly_result.iloc[0]['月份']
        diff = monthly_result.iloc[0]['毛利_diff']
        print(
            "最大波动月份: %s，毛利变化%+.2f" % (month, diff)
        )


if __name__ == "__main__":
    # 配置
    OUTPUT_DIR = "/home/yanzhuxin/guany/reports/"
    OUTPUT_REPORT_PATH = OUTPUT_DIR + "月维度前后两日波动分析报告_排除七牛CDN.md"
    # 企业微信推送配置：使用测试版
    ENABLE_WECHAT_PUSH = True
    WECHAT_WEBHOOK = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=36887f02-3fcf-46ac-a13f-69ddf0ddb595"
    USE_TEST_WEBHOOK = True  # 使用测试webhook

    # 1. 获取数据
    print("===== 开始获取数据 =====")
    # 动态筛选月份范围：
    # 如果今天 >= 3号，筛选范围为【四个月前 ~ 当月】
    # 如果今天 < 3号，筛选范围为【五个月前 ~ 上个月】
    today = datetime.now()
    if today.day >= 3:
        # 四个月前到当月
        end_month = today.strftime("%Y-%m")
        # 计算四个月前
        start_datetime = today.replace(day=1) - timedelta(days=4*30)
        start_month = start_datetime.strftime("%Y-%m")
    else:
        # 五个月前到上个月
        # 计算上个月
        end_datetime = today.replace(day=1) - timedelta(days=1)
        end_month = end_datetime.strftime("%Y-%m")
        # 计算五个月前
        start_datetime = end_datetime.replace(day=1) - timedelta(days=5*30)
        start_month = start_datetime.strftime("%Y-%m")
    
    print("筛选月份范围: %s ~ %s" % (start_month, end_month))
    
    # 获取所有月份数据，分多次获取合并
    token = get_token()
    col = None
    all_preview = []
    colnames = []
    
    # 遍历从start_month到end_month之间的所有月份
    # 生成月份列表
    current = datetime.strptime(start_month, "%Y-%m")
    end_datetime = datetime.strptime(end_month, "%Y-%m")
    while current <= end_datetime:
        current_month_str = current.strftime("%Y-%m")
        print("正在获取 %s 月份数据..." % current_month_str)
        fc = FilterCondition()
        fc.eq("合并月份", current_month_str).ne("客户_new", "七牛CDN")
        result = GuanDataFetcher.fetch_data(
            token=token, ds_id="eff95e2a2fe0048dfb9727b1", filter_condition=fc, limit=50000
        )
        if col is None:
            col = result.get("columns", [])
            colnames = [col[i]["name"] for i in range(len(col))]
        preview = result.get("preview", [])
        all_preview.extend(preview)
        # 移动到下一个月
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    df2 = pd.DataFrame(all_preview, columns=colnames)
    print("全部获取完成，共 %d 行" % len(df2))

    # 2. 数据预处理
    print("\n===== 开始数据预处理 =====")
    df = load_and_preprocess_data(df2)
    print("预处理完成")

    # 3. 获取对比日期
    latest_day, prev_day = get_compare_days(df)
    if latest_day is None or prev_day is None:
        print("❌ 数据不足两天，无法分析")
        exit(1)
    print("分析日期: %s (昨日) → %s (今日)" % (prev_day, latest_day))

    # 4. 计算月度指标对比
    print("\n===== 开始计算月度指标对比 =====")
    monthly_result = calculate_monthly_metrics(df, latest_day, prev_day)
    print("计算完成，共 %d 个月份符合条件" % len(monthly_result))

    # 5. 生成分析报告
    print("\n===== 生成分析报告 =====")
    report = generate_markdown_report(monthly_result, latest_day, prev_day, OUTPUT_REPORT_PATH)

    # 6. 推送至企业微信机器人（测试版）
    if ENABLE_WECHAT_PUSH:
        print("\n正在推送报告到企业微信测试版...")
        send_to_wechat_webhook(WECHAT_WEBHOOK, report, OUTPUT_REPORT_PATH)

    # 打印核心结论
    print("\n===== 核心结论 =====")
    total_prev_profit = monthly_result["毛利_new_prev"].sum()
    total_latest_profit = monthly_result["毛利_new_latest"].sum()
    total_profit_diff = total_latest_profit - total_prev_profit
    total_profit_rate = (total_profit_diff / total_prev_profit * 100) if total_prev_profit !=0 else np.inf
    print(
        "总毛利变化: %+.2f (%+.2f%%)" % (total_profit_diff, total_profit_rate)
    )
    if len(monthly_result) > 0:
        month = monthly_result.iloc[0]['月份']
        diff = monthly_result.iloc[0]['毛利_diff']
        print(
            "最大波动月份: %s，毛利变化%+.2f" % (month, diff)
        )

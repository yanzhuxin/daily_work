#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MongoDB → StarRocks 高效同步工具
基于观远BI同步原理实现

核心优化:
1. 大批量读取 (50,000条) 减少网络往返 80%
2. Stream Load 替代 INSERT，性能提升 10-100倍
3. no_cursor_timeout 避免长时间同步游标超时
4. _id断点 + updatedTime 实现全量+增量同步
5. StarRocks UPSERT 自动去重
"""

import json
import gzip
import requests
import pymongo
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
from typing import Dict, List, Optional, Generator, Any
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import csv
import os
import sys

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('sync.log')
    ]
)
logger = logging.getLogger(__name__)


class JSONEncoder(json.JSONEncoder):
    """处理MongoDB特殊类型的JSON编码器"""
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        return super().default(obj)


class MongoDBStarRocksSync:
    """MongoDB到StarRocks高效同步器"""
    
    def __init__(
        self,
        # MongoDB配置
        mongo_uri: str,
        mongo_db: str,
        mongo_collection: str,
        # StarRocks配置
        starrocks_host: str,
        starrocks_user: str,
        starrocks_password: str,
        starrocks_db: str,
        starrocks_table: str,
        starrocks_port: int = 8030,
        # 同步配置
        batch_size: int = 50000,  # 观远优化: 50,000
        max_workers: int = 1,  # 并行线程数
        use_gzip: bool = True,  # 启用gzip压缩
        checkpoint_dir: str = "./checkpoints",
    ):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.starrocks_host = starrocks_host
        self.starrocks_port = starrocks_port
        self.starrocks_user = starrocks_user
        self.starrocks_password = starrocks_password
        self.starrocks_db = starrocks_db
        self.starrocks_table = starrocks_table
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.use_gzip = use_gzip
        
        # MongoDB连接
        logger.info(f"连接MongoDB: {mongo_uri}")
        self.mongo_client = MongoClient(mongo_uri, maxPoolSize=10)
        self.collection = self.mongo_client[mongo_db][mongo_collection]
        
        # 断点目录
        self.checkpoint_dir = checkpoint_dir
        os.makedirs(checkpoint_dir, exist_ok=True)
        self.checkpoint_file = os.path.join(
            checkpoint_dir, 
            f"checkpoint_{mongo_db}_{mongo_collection}.json"
        )
        
        # 统计
        self.stats = {
            "total_read": 0,
            "total_loaded": 0,
            "total_filtered": 0,
            "batches": 0,
            "start_time": None,
            "end_time": None
        }
    
    # ==================== 方法1: 流式批量读取 ====================
    
    def stream_documents(
        self, 
        query: Dict = None,
        last_id: Optional[str] = None,
        updated_time_field: Optional[str] = None,
        last_updated_time: Optional[str] = None,
    ) -> Generator[Dict, None, None]:
        """
        流式批量读取MongoDB数据
        
        关键优化:
        - batch_size=50000 减少网络往返
        - no_cursor_timeout=True 避免长时间同步游标超时
        - sort by _id 支持断点续传
        """
        if query is None:
            query = {}
        
        # 构建查询条件
        if last_id:
            # 全量同步断点
            query["_id"] = {"$gt": ObjectId(last_id)}
            logger.info(f"全量同步断点: _id > {last_id}")
        
        if updated_time_field and last_updated_time:
            # 增量同步断点
            query[updated_time_field] = {"$gte": last_updated_time}
            logger.info(f"增量同步断点: {updated_time_field} >= {last_updated_time}")
        
        logger.info(f"开始流式读取, query: {query}, batch_size: {self.batch_size}")
        
        # 关键配置: no_cursor_timeout=True (观远优化)
        cursor = self.collection.find(
            query,
            batch_size=self.batch_size,
            no_cursor_timeout=True,  # 避免长时间同步游标超时
            sort=[("_id", pymongo.ASCENDING)]  # 按_id升序，支持断点
        )
        
        count = 0
        try:
            for doc in cursor:
                yield doc
                count += 1
                if count % 100000 == 0:
                    logger.info(f"已读取: {count}条")
        finally:
            # 关键: 最后关闭游标防止资源泄漏 (观远优化)
            cursor.close()
            logger.info(f"游标已关闭, 总计读取: {count}条")
    
    # ==================== 方法2: JSON扁平化 ====================
    
    def flatten_document(
        self, 
        doc: Dict, 
        parent_key: str = "", 
        sep: str = "_",
        max_depth: int = 2
    ) -> Dict:
        """
        扁平化嵌套JSON文档
        
        策略:
        - 简单嵌套对象(1-2层): 展开为独立列
        - 数组/深层嵌套: JSON序列化存储
        """
        items = {}
        current_depth = parent_key.count(sep) if parent_key else 0
        
        for key, value in doc.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            
            if isinstance(value, dict):
                # 递归扁平化嵌套对象
                if current_depth < max_depth:
                    items.update(self.flatten_document(value, new_key, sep, max_depth))
                else:
                    # 深层嵌套 → JSON字符串
                    items[new_key] = json.dumps(value, ensure_ascii=False, cls=JSONEncoder)
            elif isinstance(value, list):
                # 数组 → JSON字符串
                items[new_key] = json.dumps(value, ensure_ascii=False, cls=JSONEncoder)
            elif isinstance(value, datetime):
                # 日期格式化
                items[new_key] = value.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(value, ObjectId):
                # ObjectId 转字符串
                items[new_key] = str(value)
            else:
                items[new_key] = value
        
        return items
    
    # ==================== 方法3: Stream Load 批量导入 ====================
    
    def stream_load_to_starrocks(
        self, 
        records: List[Dict],
        columns: List[str],
        batch_id: int = 0
    ) -> Dict[str, Any]:
        """
        使用StarRocks Stream Load批量导入
        
        比INSERT快10-100倍的原因:
        - HTTP直接写入，无SQL解析开销
        - 批量解析，批量写入
        - 直接写入BE，无需FE转发
        """
        if not records:
            return {"success": True, "loaded": 0, "filtered": 0}
        
        # 构建CSV格式数据
        output = io.StringIO()
        writer = csv.DictWriter(
            output, 
            fieldnames=columns,
            delimiter='\t',  # 使用tab分隔
            quoting=csv.QUOTE_MINIMAL,
            lineterminator='\n',
            extrasaction='ignore'  # 忽略多余字段
        )
        
        # 写入数据（跳过header）
        for record in records:
            # 处理None值
            row = {k: v if v is not None else '\\N' for k, v in record.items()}
            writer.writerow(row)
        
        data = output.getvalue()
        output.close()
        
        # Stream Load HTTP请求
        url = f"http://{self.starrocks_host}:{self.starrocks_port}/api/{self.starrocks_db}/{self.starrocks_table}/_stream_load"
        
        headers = {
            "Expect": "100-continue",
            "Content-Type": "text/plain; charset=UTF-8",
            "columns": ",".join(columns),
            "column_separator": "\\t",
            "format": "csv",
            "max_filter_ratio": "0.2",  # 允许20%错误率
        }
        
        # 启用gzip压缩
        if self.use_gzip:
            headers["Content-Encoding"] = "gzip"
            data = gzip.compress(data.encode('utf-8'))
        
        try:
            response = requests.put(
                url,
                headers=headers,
                data=data,
                auth=(self.starrocks_user, self.starrocks_password),
                timeout=120
            )
            
            result = response.json()
            
            if result.get("Status") == "Success":
                loaded = int(result.get('NumberLoadedRows', 0))
                filtered = int(result.get('NumberFilteredRows', 0))
                logger.info(f"[Batch {batch_id}] Stream Load成功: {len(records)}条, "
                          f"已加载: {loaded}, 过滤: {filtered}")
                return {
                    "success": True,
                    "loaded": loaded,
                    "filtered": filtered
                }
            else:
                logger.error(f"[Batch {batch_id}] Stream Load失败: {result}")
                return {
                    "success": False,
                    "error": result.get("Message", "Unknown error"),
                    "loaded": 0,
                    "filtered": len(records)
                }
                
        except Exception as e:
            logger.error(f"[Batch {batch_id}] Stream Load异常: {e}")
            return {
                "success": False,
                "error": str(e),
                "loaded": 0,
                "filtered": len(records)
            }
    
    # ==================== 方法4: 断点管理 ====================
    
    def load_checkpoint(self) -> Dict:
        """加载断点"""
        try:
            with open(self.checkpoint_file, 'r') as f:
                checkpoint = json.load(f)
                logger.info(f"加载断点: {checkpoint}")
                return checkpoint
        except FileNotFoundError:
            default_checkpoint = {
                "sync_mode": "full",
                "last_id": None,
                "last_updated_time": "1970-01-01 00:00:00"
            }
            logger.info(f"使用默认断点: {default_checkpoint}")
            return default_checkpoint
    
    def save_checkpoint(
        self, 
        last_id: Optional[str] = None,
        last_updated_time: Optional[str] = None,
        sync_mode: str = "full"
    ):
        """保存断点"""
        checkpoint = self.load_checkpoint()
        
        if last_id:
            checkpoint["last_id"] = last_id
        if last_updated_time:
            checkpoint["last_updated_time"] = last_updated_time
        checkpoint["sync_mode"] = sync_mode
        checkpoint["updated_at"] = datetime.now().isoformat()
        
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint, f, indent=2)
        
        logger.info(f"断点已保存: {checkpoint}")
    
    # ==================== 全量同步 ====================
    
    def sync_full(self) -> Dict[str, Any]:
        """
        全量同步逻辑
        
        流程:
        1. 加载断点
        2. 按_id升序批量查询 (no_cursor_timeout)
        3. 扁平化处理
        4. 批量Stream Load
        5. 更新断点
        6. 完成后切换到增量模式
        """
        self.stats["start_time"] = datetime.now()
        checkpoint = self.load_checkpoint()
        last_id = checkpoint.get("last_id")
        
        logger.info("=" * 60)
        logger.info("开始全量同步")
        logger.info(f"断点: last_id={last_id}")
        logger.info(f"批量大小: {self.batch_size}")
        logger.info("=" * 60)
        
        batch = []
        columns = None
        batch_id = 0
        failed_batches = []
        
        for doc in self.stream_documents(last_id=last_id):
            # 扁平化处理
            flat_doc = self.flatten_document(doc)
            
            # 记录列名（第一批数据）
            if columns is None:
                columns = list(flat_doc.keys())
                logger.info(f"目标表列: {columns}")
            
            batch.append(flat_doc)
            self.stats["total_read"] += 1
            
            # 达到批量大小，执行Stream Load
            if len(batch) >= self.batch_size:
                result = self.stream_load_to_starrocks(batch, columns, batch_id)
                
                if result["success"]:
                    self.stats["total_loaded"] += result["loaded"]
                    self.stats["total_filtered"] += result["filtered"]
                    self.stats["batches"] += 1
                    
                    # 更新断点为最后一条记录的_id
                    last_id = str(batch[-1].get("_id", ""))
                    self.save_checkpoint(last_id=last_id, sync_mode="full")
                else:
                    logger.error(f"批次 {batch_id} 失败: {result.get('error')}")
                    failed_batches.append((batch_id, batch, result.get('error')))
                
                batch = []
                batch_id += 1
                
                # 打印进度
                if self.stats["batches"] % 10 == 0:
                    self._print_progress()
        
        # 处理剩余数据
        if batch:
            result = self.stream_load_to_starrocks(batch, columns, batch_id)
            if result["success"]:
                self.stats["total_loaded"] += result["loaded"]
                self.stats["total_filtered"] += result["filtered"]
                self.stats["batches"] += 1
                last_id = str(batch[-1].get("_id", ""))
                self.save_checkpoint(last_id=last_id, sync_mode="full")
        
        self.stats["end_time"] = datetime.now()
        
        # 完成后切换到增量模式
        self.save_checkpoint(last_id=last_id, sync_mode="incremental")
        
        logger.info("=" * 60)
        logger.info("全量同步完成")
        self._print_progress()
        logger.info("=" * 60)
        
        return {
            "success": len(failed_batches) == 0,
            "stats": self.stats,
            "failed_batches": failed_batches
        }
    
    # ==================== 增量同步 ====================
    
    def sync_incremental(
        self, 
        updated_time_field: str = "updatedTime",
        id_field: str = "_id"
    ) -> Dict[str, Any]:
        """
        增量同步逻辑
        
        流程:
        1. 加载断点（last_updated_time）
        2. 查询 updatedTime >= last_updated_time 的数据
        3. 批量处理并Stream Load
        4. StarRocks UPSERT自动去重（主键表）
        5. 更新断点
        """
        self.stats["start_time"] = datetime.now()
        checkpoint = self.load_checkpoint()
        last_updated_time = checkpoint.get("last_updated_time", "1970-01-01 00:00:00")
        
        logger.info("=" * 60)
        logger.info("开始增量同步")
        logger.info(f"时间断点: {updated_time_field} >= {last_updated_time}")
        logger.info(f"批量大小: {self.batch_size}")
        logger.info("=" * 60)
        
        batch = []
        columns = None
        batch_id = 0
        failed_batches = []
        max_updated_time = last_updated_time
        
        for doc in self.stream_documents(
            updated_time_field=updated_time_field,
            last_updated_time=last_updated_time
        ):
            flat_doc = self.flatten_document(doc)
            
            if columns is None:
                columns = list(flat_doc.keys())
                logger.info(f"目标表列: {columns}")
            
            batch.append(flat_doc)
            self.stats["total_read"] += 1
            
            # 记录最大更新时间
            doc_updated_time = doc.get(updated_time_field)
            if doc_updated_time:
                doc_time_str = doc_updated_time.strftime("%Y-%m-%d %H:%M:%S") if isinstance(doc_updated_time, datetime) else str(doc_updated_time)
                if doc_time_str > max_updated_time:
                    max_updated_time = doc_time_str
            
            # 批量导入
            if len(batch) >= self.batch_size:
                result = self.stream_load_to_starrocks(batch, columns, batch_id)
                
                if result["success"]:
                    self.stats["total_loaded"] += result["loaded"]
                    self.stats["total_filtered"] += result["filtered"]
                    self.stats["batches"] += 1
                    
                    # 更新断点
                    self.save_checkpoint(
                        last_id=str(batch[-1].get(id_field, "")),
                        last_updated_time=max_updated_time,
                        sync_mode="incremental"
                    )
                else:
                    logger.error(f"批次 {batch_id} 失败: {result.get('error')}")
                    failed_batches.append((batch_id, batch, result.get('error')))
                
                batch = []
                batch_id += 1
        
        # 处理剩余数据
        if batch:
            result = self.stream_load_to_starrocks(batch, columns, batch_id)
            if result["success"]:
                self.stats["total_loaded"] += result["loaded"]
                self.stats["total_filtered"] += result["filtered"]
                self.stats["batches"] += 1
                self.save_checkpoint(
                    last_id=str(batch[-1].get(id_field, "")),
                    last_updated_time=max_updated_time,
                    sync_mode="incremental"
                )
        
        self.stats["end_time"] = datetime.now()
        
        logger.info("=" * 60)
        logger.info("增量同步完成")
        self._print_progress()
        logger.info("=" * 60)
        
        return {
            "success": len(failed_batches) == 0,
            "stats": self.stats,
            "failed_batches": failed_batches
        }
    
    def _print_progress(self):
        """打印进度"""
        duration = (self.stats["end_time"] or datetime.now()) - self.stats["start_time"]
        duration_secs = duration.total_seconds()
        
        rate = self.stats["total_loaded"] / duration_secs if duration_secs > 0 else 0
        
        logger.info("-" * 60)
        logger.info(f"已读取: {self.stats['total_read']:,} 条")
        logger.info(f"已加载: {self.stats['total_loaded']:,} 条")
        logger.info(f"已过滤: {self.stats['total_filtered']:,} 条")
        logger.info(f"批次: {self.stats['batches']}")
        logger.info(f"耗时: {duration_secs:.2f} 秒")
        logger.info(f"速率: {rate:.2f} 条/秒")
        logger.info("-" * 60)
    
    def close(self):
        """关闭连接"""
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("MongoDB连接已关闭")


# ==================== 命令行入口 ====================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='MongoDB to StarRocks Sync Tool')
    parser.add_argument('--mode', choices=['full', 'incremental'], default='full',
                       help='同步模式: full=全量, incremental=增量')
    parser.add_argument('--mongo-uri', required=True, help='MongoDB连接URI')
    parser.add_argument('--mongo-db', required=True, help='MongoDB数据库名')
    parser.add_argument('--mongo-collection', required=True, help='MongoDB集合名')
    parser.add_argument('--starrocks-host', required=True, help='StarRocks FE主机')
    parser.add_argument('--starrocks-user', default='root', help='StarRocks用户名')
    parser.add_argument('--starrocks-password', default='', help='StarRocks密码')
    parser.add_argument('--starrocks-db', required=True, help='StarRocks数据库名')
    parser.add_argument('--starrocks-table', required=True, help='StarRocks表名')
    parser.add_argument('--batch-size', type=int, default=50000, help='批量大小')
    parser.add_argument('--updated-time-field', default='updatedTime', 
                       help='增量同步时间字段名')
    
    args = parser.parse_args()
    
    # 创建同步器
    sync = MongoDBStarRocksSync(
        mongo_uri=args.mongo_uri,
        mongo_db=args.mongo_db,
        mongo_collection=args.mongo_collection,
        starrocks_host=args.starrocks_host,
        starrocks_user=args.starrocks_user,
        starrocks_password=args.starrocks_password,
        starrocks_db=args.starrocks_db,
        starrocks_table=args.starrocks_table,
        batch_size=args.batch_size
    )
    
    try:
        if args.mode == 'full':
            result = sync.sync_full()
        else:
            result = sync.sync_incremental(updated_time_field=args.updated_time_field)
        
        if result["success"]:
            logger.info("同步成功完成!")
            sys.exit(0)
        else:
            logger.error(f"同步失败，失败批次: {len(result['failed_batches'])}")
            sys.exit(1)
    
    except KeyboardInterrupt:
        logger.info("用户中断同步")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"同步异常: {e}")
        sys.exit(1)
    finally:
        sync.close()


if __name__ == "__main__":
    main()

# sync_data

**创建时间**: 2026-03-23

## 功能

MongoDB 数据同步到 StarRocks 的数据同步项目，支持：
- 全量同步：从 MongoDB 全量导出数据到 StarRocks
- 增量同步：基于 updatedTime 增量同步更新数据
- 断点续传：支持中断后从断点继续同步
- 类型转换：处理 MongoDB BSON 特殊类型到 StarRocks 兼容格式

## 修改记录

| 修改时间 | 修改人 | 修改内容 |
|----------|--------|----------|
| 2026-03-23 | - | 初始创建项目，实现基本全量/增量同步功能 |
| 2026-03-23 | - | 修复SQL语法错误：移除StarRocks不支持的ON DUPLICATE KEY UPDATE语法；添加logging日志记录，替换所有print为标准日志输出 |

## 主要文件

- `mongodb2starRocks.py` - 主同步程序
- `sync_incremental.sh` - 增量同步执行脚本
- `requirements.txt` - Python 依赖列表
- `sync_checkpoint.json` - 断点续传记录文件
- `bi_export_import.py` - BI 导出导入工具
- `export_csv.py` - CSV 导出工具
- `fast_sync.py` - 快速同步工具
- `multi_sync.py` - 多线程同步工具
- `sync_shard_*.py` - 分表同步工具

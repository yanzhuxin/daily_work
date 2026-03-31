#!/bin/bash
# 增量同步执行脚本
WORK_DIR="/home/yanzhuxin/sync_data"
LOG_DIR="${WORK_DIR}/logs"
mkdir -p ${LOG_DIR}
LOG_FILE="${LOG_DIR}/sync_incremental_$(date +%Y%m%d_%H%M%S).log"

cd ${WORK_DIR}
echo "=== 开始增量同步，时间: $(date)" >> ${LOG_FILE}
python3 mongodb2starRocks.py incremental >> ${LOG_FILE} 2>&1
echo "同步完成，时间: $(date)" >> ${LOG_FILE}

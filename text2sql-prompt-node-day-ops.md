# text2sql 提示词模板 - node_day_ops_wide_full 日维度带宽成本毛利表

## 表说明

这是一张 CDN 节点日维度数据表，存储了每个节点每天的带宽、成本、收入、毛利等数据，数据来自 MongoDB 同步到 StarRocks。

## 完整建表语句

```sql
-- 完整建表语句 - node_day_ops_wide_full
-- MongoDB nodeDayOpsWide 全字段同步到 StarRocks
-- 包含所有遗漏字段: idcId, idcBandwidth, transProvRate, isVm, isCloudVm, scheduleISPs, settlePeriodType

CREATE TABLE IF NOT EXISTS test.node_day_ops_wide_full (
    customerId           bigint               NOT NULL COMMENT '客户 Id',
    day                 date                 NOT NULL COMMENT '日期，格式：YYYY-MM-DD',
    nodeId              varchar(64)         NOT NULL COMMENT '节点 Id',
    analyzePeak95       bigint              COMMENT '分析日95带宽，单位：bps',
    baseInfo_channelId  bigint              COMMENT '基础信息 - 通道Id',
    baseInfo_signatoryId bigint             COMMENT '基础信息 - 签约Id',
    baseInfo_bandwidth  double              COMMENT '基础信息 - 带宽',
    buildBandwidth      double              COMMENT '原始建设带宽，单位：Mbps',
    city                varchar(256)        COMMENT '城市',
    cost_guaranteedRate double              COMMENT '成本 - 保底比例',
    cost_priceItemId    varchar(64)         COMMENT '成本 - 价格项Id',
    cost_priceItemName  varchar(128)        COMMENT '成本 - 价格项名称',
    cost_priceType      varchar(32)         COMMENT '成本 - 价格类型',
    cost_price          decimal(18,4)        COMMENT '成本 - 价格',
    cost_priceAfterBonus decimal(18,4)       COMMENT '成本 - bonus后价格',
    cost_measure       double              COMMENT '成本 - 计量',
    cost_original      decimal(18,4)        COMMENT '成本 - 原价',
    cost_bonus          decimal(18,4)        COMMENT '成本 - bonus',
    cost_slaDeduction  decimal(18,4)        COMMENT '成本 - SLA扣款',
    cost_tobaDeduction  decimal(18,4)       COMMENT '成本 - 通道费扣款',
    cost_settlement     decimal(18,4)        COMMENT '成本 - 结算',
    cost_adjustmentAmount decimal(18,4)      COMMENT '成本 - 调整金额',
    cost_finalAmount    decimal(18,4)        COMMENT '成本 - 最终金额',
    customerName        varchar(128)        COMMENT '业务名称',
    deliveryType        varchar(32)         COMMENT '资源交付类型',
    evening20To23Avg    bigint              COMMENT '20:00-23:00平均带宽，单位：bps',
    eveningAvg         bigint              COMMENT '晚高峰(18:00~23:59)平均带宽，单位：bps',
    eveningPeak95       bigint              COMMENT '晚高峰95带宽，单位：bps',
    isBanTransProv      boolean             COMMENT '是否禁止跨省调度',
    isp                varchar(32)         COMMENT '运营商',
    name                varchar(128)        COMMENT '节点名称',
    natType             varchar(32)         COMMENT 'NAT网络类型',
    nodeTags            json                 COMMENT '节点标签数组(JSON)',
    nodeType            varchar(32)         COMMENT '节点类型',
    os                  varchar(32)         COMMENT '操作系统',
    peak95              bigint              COMMENT '日95带宽，单位：bps',
    peak95Ratio         int                 COMMENT '95利用率，百分比',
    peak95Time          datetime            COMMENT '日95带宽时间戳',
    peakMaxRatio        int                 COMMENT '峰值利用率',
    priceNumber         varchar(32)         COMMENT '价格编号',
    profit_profitAmount  decimal(18,4)        COMMENT '毛利 - 毛利金额',
    profit_profitRate   decimal(18,6)        COMMENT '毛利 - 毛利率',
    profit_estimatedProfitAmount decimal(18,4) COMMENT '毛利 - 预估毛利金额',
    profit_estimatedProfitRate decimal(18,6) COMMENT '毛利 - 预估毛利率',
    -- 新增遗漏字段
    idcId               varchar(64)         COMMENT '机房 Id',
    idcBandwidth        double              COMMENT '机房建设带宽，单位：Mbps',
    transProvRate        double              COMMENT '跨省调度比例',
    isVm                boolean             COMMENT '是否为虚拟机',
    isCloudVm           boolean             COMMENT '是否为云主机',
    scheduleISPs        json                 COMMENT '实际业务调度运营商列表(JSON)',
    settlePeriodType    varchar(32)         COMMENT '结算周期类型',
    -- 原有后续字段
    province            varchar(32)         COMMENT '省份',
    purchaserName        varchar(64)         COMMENT '采购员姓名',
    quantityEnd         decimal(18,4)        COMMENT '阶梯结束值',
    quantityType        varchar(32)         COMMENT '阶梯阈值类型',
    realISP             varchar(64)         COMMENT '真实运营商',
    resourceType        varchar(32)         COMMENT '客户侧资源类型',
    revenue_guaranteedRate double            COMMENT '收入 - 保底比例',
    revenue_priceItemId  varchar(64)         COMMENT '收入 - 价格项Id',
    revenue_priceItemName varchar(128)        COMMENT '收入 - 价格项名称',
    revenue_price       decimal(18,4)        COMMENT '收入 - 价格',
    revenue_measure     double              COMMENT '收入 - 计量',
    revenue_coefficientMeasure double      COMMENT '收入 - 系数计量',
    revenue_amount      decimal(18,4)        COMMENT '收入 - 金额',
    revenue_finalAmount decimal(18,4)        COMMENT '收入 - 最终金额',
    revenue_estimatedFinalAmount decimal(18,4) COMMENT '收入 - 预估最终金额',
    signatoryName        varchar(128)        COMMENT '签约客户名称',
    snapshotTime         datetime            COMMENT '数据快照时间',
    stage               varchar(32)         COMMENT '业务状态',
    stairType           varchar(32)         COMMENT '阶梯类型',
    stairs              json                 COMMENT '阶梯价格列表(JSON)',
    state               varchar(32)         COMMENT '在线状态',
    tcpNatType          varchar(32)         COMMENT 'TCP NAT类型',
    udpNatType          varchar(32)         COMMENT 'UDP NAT类型',
    unEveningAvg        bigint              COMMENT '非晚高峰平均带宽，单位：bps',
    updatedTime         datetime            COMMENT '更新时间',
    vendorId            bigint               COMMENT '供应商 Id',
    vendorSuggestCustomers json               COMMENT '矿主期望业务客户列表(JSON)',
    virtualCustomers     json                 COMMENT '虚拟业务客户列表(JSON)',
    webPort             varchar(32)         COMMENT '业务端口',
    webPortResult        varchar(32)         COMMENT '业务端口结果'
) ENGINE=OLAP
DUPLICATE KEY(customerId, day, nodeId)
DISTRIBUTED BY HASH(customerId) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);
```

---

## 你的任务

你是一位专业的 StarRocks SQL 分析师，根据用户的自然语言问题，生成一条**可直接执行**的 SQL 查询语句。

### 🔐 严格遵守规则

1. **⚠️ 绝对禁止查询明细数据**，**所有查询必须使用 `GROUP BY` 进行聚合**
2. **⚠️ 只输出聚合统计结果**，不允许返回多条明细
3. 查询结果必须满足结构：`SELECT aggregation... FROM test.node_day_ops_wide_full GROUP BY grouping...`
4. 利用 COMMENT 理解每个字段的业务含义
5. 生成的 SQL 需要符合 StarRocks 语法
6. 所有字段名必须和建表语句一致
7. 添加适当的排序，比如按日期排序、按汇总金额排序
8. **只输出完整可执行的 SQL**，不要额外解释

### 💡 常见聚合场景参考

| 统计维度 | GROUP BY 写法 | 常用聚合 |
|---------|-------------|---------|
| 按日统计 | `GROUP BY day` | `SUM(profit_profitAmount)`, `SUM(revenue_amount)`, `SUM(cost_finalAmount)`, `COUNT(DISTINCT nodeId)` |
| 按运营商 | `GROUP BY isp` | 同上 |
| 按客户 | `GROUP BY customerId, customerName` | 同上 |
| 按机房 | `GROUP BY idcId` | 需要加 `WHERE idcId IS NOT NULL` |
| 按省份 | `GROUP BY province` | 同上 |
| 按是否虚拟机 | `GROUP BY isVm` | 同上 |

---

## 用户问题

---

**[在这里替换为用户的问题]**

# NodeDayOpsWide 数据同步脚本

本脚本用于从 MongoDB 导出 `nodeDayOpsWide` 集合数据，并同步到 StarRocks 的 `test.node_day_ops_wide_full` 表。

## MongoDB 原始结构体

原始 Go 结构体定义如下：

```go
NodeDayOpsWide struct {
    Id                     string                    `bson:"_id,omitempty" json:"id"`                                                  // _id
    NodeId                 string                    `bson:"nodeId"        json:"nodeId"`                                              // 节点 Id
    Day                    string                    `bson:"day"           json:"day"`                                                 // 日期，格式：2006-01-02
    CustomerId             uint32                    `bson:"customerId"    json:"customerId"`                                          // 客户 Id
    VendorId               uint32                    `bson:"vendorId"      json:"vendorId"`                                            // 供应商 Id
    BaseInfo               NodeBase                  `bson:"baseInfo"       json:"baseInfo"`                                           // 基础信息
    Cost                   *VendorCost               `bson:"cost"           json:"cost"`                                               // 成本
    Revenue                *CustomerRevenue          `bson:"revenue"        json:"revenue"`                                            // 收入
    Profit                 *ProfitDetail             `bson:"profit"         json:"profit"`                                             // 毛利明细
    UpdatedTime            time.Time                 `bson:"updatedTime"   json:"updatedTime"`                                         // 更新时间
    Name                   string                    `bson:"name,omitempty" json:"name,omitempty"`                                     // 节点名称
    NodeType               string                    `bson:"nodeType,omitempty" json:"nodeType,omitempty"`                             // 节点类型
    PurchaserName          string                    `bson:"purchaserName,omitempty" json:"purchaserName,omitempty"`                   // 采购员
    CustomerName           string                    `bson:"customerName,omitempty" json:"customerName,omitempty"`                     // 业务名称
    SignatoryName          string                    `bson:"signatoryName,omitempty" json:"signatoryName,omitempty"`                   // 客户名称
    IdcId                  string                    `bson:"idcId,omitempty" json:"idcId,omitempty"`                                   // 机房 Id
    IdcBandwidth           float64                   `bson:"idcBandwidth,omitempty" json:"idcBandwidth,omitempty"`                     // 机房建设带宽，单位：Mbps
    BuildBandwidth         float64                   `bson:"buildBandwidth,omitempty" json:"buildBandwidth,omitempty"`                 // 原始建设带宽，单位：Mbps
    WebPort                sharedmodel.WebPort       `bson:"webPort,omitempty" json:"webPort,omitempty"`                               // 业务端口
    WebPortResult          sharedmodel.WebPortResult `bson:"webPortResult,omitempty" json:"webPortResult,omitempty"`                   // 业务端口结果
    City                   string                    `bson:"city,omitempty" json:"city,omitempty"`                                     // 城市
    Province               string                    `bson:"province,omitempty" json:"province,omitempty"`                             // 省份
    NatType                sharedmodel.NatType       `bson:"natType,omitempty" json:"natType,omitempty"`                               // 网络类型
    TcpNatType             sharedmodel.NatType       `bson:"tcpNatType,omitempty" json:"tcpNatType,omitempty"`                         // TCP NAT 类型
    UdpNatType             sharedmodel.NatType       `bson:"udpNatType,omitempty" json:"udpNatType,omitempty"`                         // UDP NAT 类型
    SnapshotTime           time.Time                 `bson:"snapshotTime,omitempty" json:"snapshotTime,omitempty"`                     // 快照时间
    State                  string                    `bson:"state,omitempty" json:"state,omitempty"`                                   // 在线状态
    ISP                    string                    `bson:"isp,omitempty" json:"isp,omitempty"`                                       // 运营商
    RealISP                string                    `bson:"realISP,omitempty" json:"realISP,omitempty"`                               // 真实运营商
    Stage                  string                    `bson:"stage,omitempty" json:"stage,omitempty"`                                   // 业务状态
    ResourceType           sharedmodel.ResourceType  `bson:"resourceType,omitempty" json:"resourceType,omitempty"`                     // 客户侧资源类型
    DeliveryType           sharedmodel.DeliveryType  `bson:"deliveryType,omitempty" json:"deliveryType,omitempty"`                     // 资源类型
    VendorSuggestCustomers []uint32                  `bson:"vendorSuggestCustomers,omitempty" json:"vendorSuggestCustomers,omitempty"` // 矿主期望业务
    VirtualCustomers       []uint32                  `bson:"virtualCustomers,omitempty" json:"virtualCustomers,omitempty"`             // 虚拟业务
    IsBanTransProv         bool                      `bson:"isBanTransProv,omitempty" json:"isBanTransProv,omitempty"`                 // 是否禁止跨省调度
    TransProvRate          float64                   `bson:"transProvRate,omitempty" json:"transProvRate,omitempty"`                   // 跨省调度比例
    IsVm                   bool                      `bson:"isVm,omitempty" json:"isVm,omitempty"`                                     // 是否为虚拟机
    IsCloudVm              bool                      `bson:"isCloudVm,omitempty" json:"isCloudVm,omitempty"`                           // 是否为云主机
    Os                     string                    `bson:"os,omitempty" json:"os,omitempty"`                                         // 操作系统
    ScheduleISPs           []string                  `bson:"scheduleISPs,omitempty" json:"scheduleISPs,omitempty"`                     // 实际业务调度的运营商
    NodeTags               []sharedmodel.NodeTag     `bson:"nodeTags,omitempty" json:"nodeTags,omitempty"`                             // 节点标签
    SettlePeriodType       string                    `bson:"settlePeriodType,omitempty" json:"settlePeriodType,omitempty"`             // 结算周期类型
    StairType              string                    `bson:"stairType,omitempty" json:"stairType,omitempty"`                           // 阶梯类型
    QuantityType           string                    `bson:"quantityType,omitempty" json:"quantityType,omitempty"`                     // 阶梯阈值类型
    Stairs                 []*sharedmodel.PriceStair `bson:"stairs,omitempty" json:"stairs,omitempty"`                                 // 阶梯
    QuantityEnd            decimal.Decimal           `bson:"quantityEnd,omitempty" json:"quantityEnd,omitempty"`                       // 阶梯结束值
    Peak95                 int64                     `bson:"peak95,omitempty" json:"peak95,omitempty"`                                 // 日95带宽，单位：bps
    Peak95Time             time.Time                 `bson:"peak95Time,omitempty" json:"peak95Time,omitempty"`                         // 日95时间
    PeakMaxRatio           int64                     `bson:"peakMaxRatio,omitempty" json:"peakMaxRatio,omitempty"`                     // 分支利用率，百分比
    Peak95Ratio            int64                     `bson:"peak95Ratio,omitempty" json:"peak95Ratio,omitempty"`                       // 95利用率，百分比
    AnalyzePeak95          int64                     `bson:"analyzePeak95,omitempty" json:"analyzePeak95,omitempty"`                   // 分析日95带宽，单位：bps
    EveningPeak95          int64                     `bson:"eveningPeak95,omitempty" json:"eveningPeak95,omitempty"`                   // 晚高峰95带宽(18:00~23:59)，单位：bps
    EveningAvg             int64                     `bson:"eveningAvg,omitempty" json:"eveningAvg,omitempty"`                         // 晚高峰平均带宽(18:00~23:59)，单位：bps
    UnEveningAvg           int64                     `bson:"unEveningAvg,omitempty" json:"unEveningAvg,omitempty"`                     // 非晚高峰平均带宽(00:00~17:59)，单位：bps
    Evening20To23Avg       int64                     `bson:"evening20To23Avg,omitempty" json:"evening20To23Avg,omitempty"`             // 20:00~23:00时间段平均带宽，单位：bps
    PriceNumber            string                    `bson:"priceNumber,omitempty" json:"priceNumber,omitempty"`                       // 价格编号
}
```

## 表结构对比

根据 StarRocks 现代表结构对比结果：

- 目标表 `test.node_day_ops_wide_full` 已有 81 个字段
- 移除了 `_id` 字段，保持与现有表结构一致
- 所有嵌套字段 `baseInfo`, `cost`, `revenue`, `profit` 均已展开为下划线分隔格式

## 同步脚本

```bash
#!/bin/bash

/home/qboxserver/mongodb-replset-QSD_L-37017/_package/bin/mongoexport \
  --uri mongodb://jarvis_r:f9303f63772977bffcb915060f6d2ace@10.34.143.25:37017/jarvis \
  --collection nodeDayOpsWide \
  --fields \
nodeId,\
day,\
customerId,\
vendorId,\
baseInfo.channelId,\
baseInfo.signatoryId,\
baseInfo.bandwidth,\
cost.guaranteedRate,\
cost.priceItemId,\
cost.priceItemName,\
cost.priceType,\
cost.price,\
cost.priceAfterBonus,\
cost.measure,\
cost.original,\
cost.bonus,\
cost.slaDeduction,\
cost.tobaDeduction,\
cost.settlement,\
cost.adjustmentAmount,\
cost.finalAmount,\
revenue.guaranteedRate,\
revenue.priceItemId,\
revenue.priceItemName,\
revenue.price,\
revenue.measure,\
revenue.coefficientMeasure,\
revenue.amount,\
revenue.finalAmount,\
revenue.estimatedFinalAmount,\
profit.profitAmount,\
profit.profitRate,\
profit.estimatedProfitAmount,\
profit.estimatedProfitRate,\
updatedTime,\
name,\
nodeType,\
purchaserName,\
customerName,\
signatoryName,\
idcId,\
idcBandwidth,\
buildBandwidth,\
webPort,\
webPortResult,\
city,\
province,\
natType,\
tcpNatType,\
udpNatType,\
snapshotTime,\
state,\
isp,\
realISP,\
stage,\
resourceType,\
deliveryType,\
vendorSuggestCustomers,\
virtualCustomers,\
isBanTransProv,\
transProvRate,\
isVm,\
isCloudVm,\
os,\
scheduleISPs,\
nodeTags,\
settlePeriodType,\
stairType,\
quantityType,\
stairs,\
quantityEnd,\
peak95,\
peak95Time,\
peakMaxRatio,\
peak95Ratio,\
analyzePeak95,\
eveningPeak95,\
eveningAvg,\
unEveningAvg,\
evening20To23Avg,\
priceNumber \
  --sort "{day: -1}" \
  --out node_day_ops_b.json

sed -E 's/\{"\$date":"([^"]+)"\}/"\1"/g' node_day_ops_b.json > node_day_ops.json
zstd -f node_day_ops.json
/disk4/superset_PCDN-18011/linux_amd64_proteusctl import \
  -f ./node_day_ops.json.zst \
  --host=http://proteus.jf-logverse.internal.qiniu.io \
  --bucket=l-db \
  --database=test \
  --table=node_day_ops_wide_full \
  --partitions "day" \
  --unique_id $(sha256sum ./node_day_ops.json.zst  | awk '{ print $1 }') \
  --event_time=$(date "+%Y%m%d%H00") \
  --compress=zstd \
  --type=json \
  --ak="4xfYw_7NTmJYGCHzaw5J0A7MjkXAyJydMOxITZA-" \
  --sk="hdyCvp8VDmb6fviON8_3u_o0s68pdPHhZIypY5MF"

rm node_day_ops*
```

## 流程说明

1. **导出**：使用 `mongoexport` 从 MongoDB 导出指定字段
2. **日期处理**：使用 `sed` 将 MongoDB 的 `{"$date":"..."}` 格式转换为纯日期字符串
3. **压缩**：使用 `zstd` 压缩 JSON 文件
4. **导入**：使用 `proteusctl` 导入到 Proteus 数据平台，最终落到 StarRocks
5. **清理**：删除临时文件

## 字段说明

- 共导出 **81** 个字段，完全匹配 `test.node_day_ops_wide_full` 现有结构
- 嵌套对象使用点号展开：`baseInfo.channelId` → StarRocks 中为 `baseInfo_channelId`
- 数组类型（`vendorSuggestCustomers`, `virtualCustomers` 等）保持 JSON 格式
- 时间字段自动通过 sed 处理格式

## 修改记录

| 日期 | 修改人 | 说明 |
|------|--------|------|
| 2026-04-02 | opencode | 初始创建，基于现有 StarRocks 表结构生成 |
| 2026-04-02 | opencode | 增加 MongoDB 原始结构体说明 |
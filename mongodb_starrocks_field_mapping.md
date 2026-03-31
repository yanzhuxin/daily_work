# MongoDB → StarRocks 字段映射表

以下是 `node_day_ops_wide_full` 表中，StarRocks字段与MongoDB原始字段的对应关系：

| StarRocks字段 | MongoDB原始字段 | 说明 |
|--------------|----------------|------|
| customerId | customerId | 直接映射 |
| day | day | 直接映射 |
| nodeId | nodeId | 直接映射 |
| analyzePeak95 | analyzePeak95 | 直接映射 |
| baseInfo_channelId | baseInfo.channelId | baseInfo嵌套展开 |
| baseInfo_signatoryId | baseInfo.signatoryId | baseInfo嵌套展开 |
| baseInfo_bandwidth | baseInfo.bandwidth | baseInfo嵌套展开 |
| buildBandwidth | buildBandwidth | 直接映射 |
| city | city | 直接映射 |
| cost_guaranteedRate | cost.guaranteedRate | cost嵌套展开 |
| cost_priceItemId | cost.priceItemId | cost嵌套展开 |
| cost_priceItemName | cost.priceItemName | cost嵌套展开 |
| cost_priceType | cost.priceType | cost嵌套展开 |
| cost_price | cost.price | cost嵌套展开 |
| cost_priceAfterBonus | cost.priceAfterBonus | cost嵌套展开 |
| cost_measure | cost.measure | cost嵌套展开 |
| cost_original | cost.original | cost嵌套展开 |
| cost_bonus | cost.bonus | cost嵌套展开 |
| cost_slaDeduction | cost.slaDeduction | cost嵌套展开 |
| cost_tobaDeduction | cost.tobaDeduction | cost嵌套展开 |
| cost_settlement | cost.settlement | cost嵌套展开 |
| cost_adjustmentAmount | cost.adjustmentAmount | cost嵌套展开 |
| cost_finalAmount | cost.finalAmount | cost嵌套展开 |
| customerName | customerName | 直接映射 |
| deliveryType | deliveryType | 直接映射 |
| evening20To23Avg | evening20To23Avg | 直接映射 |
| eveningAvg | eveningAvg | 直接映射 |
| eveningPeak95 | eveningPeak95 | 直接映射 |
| isBanTransProv | isBanTransProv | 直接映射 |
| isp | isp | 直接映射 |
| name | name | 直接映射 |
| natType | natType | 直接映射 |
| nodeTags | nodeTags | 直接映射（列表→JSON） |
| nodeType | nodeType | 直接映射 |
| os | os | 直接映射 |
| peak95 | peak95 | 直接映射 |
| peak95Ratio | peak95Ratio | 直接映射 |
| peak95Time | peak95Time | 直接映射 |
| peakMaxRatio | peakMaxRatio | 直接映射 |
| priceNumber | priceNumber | 直接映射 |
| profit_profitAmount | profit.profitAmount | profit嵌套展开 |
| profit_profitRate | profit.profitRate | profit嵌套展开 |
| profit_estimatedProfitAmount | profit.estimatedProfitAmount | profit嵌套展开 |
| profit_estimatedProfitRate | profit.estimatedProfitRate | profit嵌套展开 |
| idcId | idcId | 新增字段 |
| idcBandwidth | idcBandwidth | 新增字段 |
| transProvRate | transProvRate | 新增字段 |
| isVm | isVm | 新增字段 |
| isCloudVm | isCloudVm | 新增字段 |
| scheduleISPs | scheduleISPs | 新增字段 |
| settlePeriodType | settlePeriodType | 新增字段 |
| province | province | 直接映射 |
| purchaserName | purchaserName | 直接映射 |
| quantityEnd | quantityEnd | 直接映射 |
| quantityType | quantityType | 直接映射 |
| realISP | realISP | 直接映射 |
| resourceType | resourceType | 直接映射 |
| revenue_guaranteedRate | revenue.guaranteedRate | revenue嵌套展开 |
| revenue_priceItemId | revenue.priceItemId | revenue嵌套展开 |
| revenue_priceItemName | revenue.priceItemName | revenue嵌套展开 |
| revenue_price | revenue.price | revenue嵌套展开 |
| revenue_measure | revenue.measure | revenue嵌套展开 |
| revenue_coefficientMeasure | revenue.coefficientMeasure | revenue嵌套展开 |
| revenue_amount | revenue.amount | revenue嵌套展开 |
| revenue_finalAmount | revenue.finalAmount | revenue嵌套展开 |
| revenue_estimatedFinalAmount | revenue.estimatedFinalAmount | revenue嵌套展开 |
| signatoryName | signatoryName | 直接映射 |
| snapshotTime | snapshotTime | 直接映射 |
| stage | stage | 直接映射 |
| stairType | stairType | 直接映射 |
| stairs | stairs | 直接映射（列表→JSON） |
| state | state | 直接映射 |
| tcpNatType | tcpNatType | 直接映射 |
| udpNatType | udpNatType | 直接映射 |
| unEveningAvg | unEveningAvg | 直接映射 |
| updatedTime | updatedTime | 直接映射 |
| vendorId | vendorId | 直接映射 |
| vendorSuggestCustomers | vendorSuggestCustomers | 直接映射（列表→JSON） |
| virtualCustomers | virtualCustomers | 直接映射（列表→JSON） |
| webPort | webPort | 直接映射 |
| webPortResult | webPortResult | 直接映射 |

## 总结

- **4个嵌套对象**：`baseInfo`, `cost`, `profit`, `revenue` → 全部展开为下划线分隔的扁平字段
- **新增8个字段** 在原 `node_day_ops_wide` 基础上增加了 `idcId` 等8个字段
- **列表类型** 保持JSON字符串格式存储在StarRocks中
- 所有字段**一一对应**，顺序完全一致，不存在错位
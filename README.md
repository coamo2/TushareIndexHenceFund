# TushareIndexHenceFund
从tushare 获取沪深300、上证50、中证500、创业板指数增强基金的日线行情，并计算信息比率，作为指数增强基金的筛选指标

主要有以下几个步骤

1、沪深300、上证50、中证500、创业板指对应的指数增加基金列表

2、根据基金列表去除C类份额，保留原始份额

3、利用tushare数据接口获取基金的复权单位净值

4、利用tushare数据接口获取基准指数的日线行情（本来应该用全收益指数更合理，但是tushare没有全收益指数的数据）

5、更新数据，计算信息比率并排序，作为基金筛选的指标

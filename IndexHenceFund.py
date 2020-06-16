import os
import tushare as ts
import pandas as pd
import time
import numpy as np
# import datetime




def GetFundDailyTradeData(fundCode, tsConn):
    """从Tushare获取单只基金日线数据"""
    fundCode_OF = fundCode
    fundCode_SZ = fundCode.replace("OF", "SZ")
    fundCode_SH = fundCode.replace("OF", "SH")

    df = tsConn.fund_nav(ts_code=fundCode_OF)
    if len(df) == 0:
        df = tsConn.fund_nav(ts_code=fundCode_SZ)
        if len(df) == 0:
            df = tsConn.fund_nav(ts_code=fundCode_SH)
            if len(df) == 0:
                return None

    df["fundCode"] = fundCode_OF
    df = df[["ts_code", "fundCode", "end_date", "adj_nav"]]
    df.columns = ["tsCode", "fundCode", "date", "adjNav"]
    df = df.drop_duplicates()
    return df.sort_values(by=["date"]).reset_index(drop=True)


def GetIR(fundData, nlookbackLs=list(np.array([12, 18, 24, 30, 36]) * 21)):
    """计算信息比率"""
    colNames = list(fundData.columns)
    fundData["excessRetDaily"] = fundData["adjNav"].pct_change() - fundData["benchmarkClose"].pct_change()

    for nlookback in nlookbackLs:
        fundData["trackingErr"] = fundData["excessRetDaily"].rolling(window=nlookback,
                                                                     min_periods=nlookback).std() * np.sqrt(252)
        fundData["fundRet"] = (fundData["adjNav"] / fundData["adjNav"].shift(nlookback)) ** (252 / nlookback) - 1
        fundData["benchmarkRet"] = (fundData["benchmarkClose"] / fundData["benchmarkClose"].shift(nlookback)) ** (
                    252 / nlookback) - 1
        fundData["IR" + str(nlookback)] = (fundData["fundRet"] - fundData["benchmarkRet"]) / fundData["trackingErr"]
        # fundData["IR"+str(nlookback)] = fundData["excessRetDaily"].rolling(window=nlookback, min_periods=nlookback).mean() / fundData["trackingErr"]
        colNames += ["IR" + str(nlookback)]
    return fundData[colNames]



class IndexEnhanceFundSelect():
    def __init__(self, savePath, token):
        """
        初始化
        :param savePath: 数据和结果的保存路径
        :parm token: tushare token
        """
        self.savePath = savePath
        self.token = token
        self.saveSignalPath = os.path.join(self.savePath, "基金筛选结果")

        # 如果没有创建目录，则创建数据目录
        os.makedirs(self.saveSignalPath, exist_ok=True)

        # 连接Tushare
        try:
            ts.set_token(self.token)        # 设置token
            self.tsConn = ts.pro_api()  # 初始化数据接口
            print("Connect to Tushare successfully.")
        except:
            print("Connect to Tushare failed.")

    def GetFundList(self, fundListFile):
        """
        导入、整理指数增强基金列表
        :param fundListFile: 指数增强基金列表所在文件
        """
        fundList = pd.read_excel(fundListFile, sheet_name=None)
        fundInfo = []
        for idx in fundList.keys():
            idxFundList = fundList[idx][
                ['证券代码', '证券简称', '跟踪指数代码', '是否初始基金', '是否分级基金', '基金成立日', '基金规模(合计)\r\n[交易日期] 最新\r\n[单位] 亿元']].copy()
            idxFundList["benchmarkName"] = idx
            idxFundList.columns = ["fundCode", "fundName", "benchmarkCode", "isInitialFund", "isStructuredFund",
                                   "establishDate", "latestFundScale", "benchmarkName"]

            # 如果同时有A类和C类, 去掉A类
            keepIdx = []
            for i in range(len(idxFundList)):
                fundName = idxFundList["fundName"].values[i]
                if fundName[-1] == "A":
                    if (fundName[:-1] + "C") in idxFundList["fundName"].values:
                        keepIdx.append(False)
                    else:
                        keepIdx.append(True)
                elif fundName[-2:] == "AB":
                    if (fundName[:-2] + "C") in idxFundList["fundName"].values:
                        keepIdx.append(False)
                    else:
                        keepIdx.append(True)
                else:
                    keepIdx.append(True)

            fundInfo.append(idxFundList[keepIdx].copy())

        fundInfo = pd.concat(fundInfo).reset_index(drop=True)
        fundInfo.to_csv(os.path.join(self.savePath, "整理后基金列表.csv"), encoding="gbk")
        print("指数增强基金列表整理完成")
        self.fundInfo = fundInfo
        return fundInfo


    def GetFundNavData(self, fundInfo):
        """
        获取基金净值数据
        :param fundInfo: 基金列表
        """
        errorFundCode = []
        data = []
        for count, fundCode in enumerate(fundInfo["fundCode"].values):
            msg = str(count + 1) + "/" + str(len(fundInfo)) + ": " + fundCode
            try:
                res = GetFundDailyTradeData(fundCode=fundCode, tsConn=self.tsConn)
            except:
                try:
                    res = GetFundDailyTradeData(fundCode=fundCode, tsConn=self.tsConn)
                except:
                    res = GetFundDailyTradeData(fundCode=fundCode, tsConn=self.tsConn)

            if res is None:
                errorFundCode.append(fundCode)
                print(msg + "    failed")
            else:
                data.append(res)
                print(msg + "    success, use " + res["tsCode"].values[0])
            time.sleep(3)


        errorFundCode = fundInfo.loc[[(x in errorFundCode) for x in fundInfo["fundCode"]]].copy()
        errorFundCode.to_csv(os.path.join(self.savePath, "Total_" + str(len(errorFundCode)) + "_ErrorFund.csv"))

        fundNavData = pd.concat(data)
        fundNavData = fundNavData.merge(fundInfo, left_on=["fundCode"], right_on=["fundCode"], how="left")
        print("基金日线行情更新完成(失败" + str(len(errorFundCode)) + "个, 共" + str(len(fundInfo)) + "个)")
        self.fundNavData = fundNavData
        return fundNavData


    def GetBenchmarkData(self):
        """获取基准指数日线行情(应该用全收益指数)"""
        cybz = self.tsConn.index_daily(ts_code='399006.SZ')[['ts_code', 'trade_date', 'close']]
        sz50 = self.tsConn.index_daily(ts_code='000016.SH')[['ts_code', 'trade_date', 'close']]
        hs300 = self.tsConn.index_daily(ts_code='000300.SH')[['ts_code', 'trade_date', 'close']]
        zz500 = self.tsConn.index_daily(ts_code='000905.SH')[['ts_code', 'trade_date', 'close']]

        cybz.columns = ['benchmarkCode', 'date', 'benchmarkClose']
        sz50.columns = ['benchmarkCode', 'date', 'benchmarkClose']
        hs300.columns = ['benchmarkCode', 'date', 'benchmarkClose']
        zz500.columns = ['benchmarkCode', 'date', 'benchmarkClose']

        self.benchmarkData = pd.concat([cybz, sz50, hs300, zz500])
        print("基准指数日线行情更新完成")
        return self.benchmarkData


    def UpdateDailyData(self, fundListFile):
        """
        更新日线行情
        :param fundListFile: 指数增强基金列表所在文件
        """
        fundInfo = self.GetFundList(fundListFile=fundListFile)
        fundNavData = self.GetFundNavData(fundInfo=fundInfo)
        benchmarkData = self.GetBenchmarkData()

        dailyData = fundNavData.merge(benchmarkData, left_on=["date", 'benchmarkCode'], right_on=["date", 'benchmarkCode'], how="left")
        dailyData = dailyData.dropna()
        dailyData.to_csv(os.path.join(self.savePath, "日线行情.csv"), index=False)
        print("日线行情更新完成")
        return dailyData


    def SelectFund(self, dailyData, nlookbackLs=[24*21, 30*21, 36*21]):
        """
        计算信息比率并选择指数增强基金
        :param dailyData: 日线行情数据
        :param nlookbackLs: 计算信息比率回看的长度
        """
        dailyData = dailyData.sort_values(by=['benchmarkCode', 'fundCode', 'date']).reset_index(drop=True)
        IRData = dailyData.groupby("fundCode").apply(lambda x: GetIR(fundData=x, nlookbackLs=nlookbackLs))

        IRData.to_csv(os.path.join(self.savePath, "信息比率.csv"), index=False)
        print("信息比率计算完成")
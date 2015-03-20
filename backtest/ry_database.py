# -*- coding: utf-8 -*-
"""
Created on Fri Feb 27 19:04:30 2015

@author: Brandon
"""
import pymssql
import sqlalchemy
import pandas as pd
import numpy as np
import datetime as dt
import os 
from dateutil import relativedelta
from collections import OrderedDict
from enum import Enum


    
'''
数据库有如下几种基本操作可以归纳：
    1. connect
    2. insert
    3. delete
    4. replace
    5. check
    6. load
    
形成数据库时有两个步骤：
    1. 批量导入数据形成历史数据库: 主要是批量insert
    2. 每日更新数据库：insert

数据库维护：check and replace

所有数据的抽象：
品种：IF
SecID：IF1005

'''
def getMSSQLConnection(host='localhost',user='sa',pwd='871108',database = 'CFFEX_IF_Quote'):
    '''
    返回一个pymssql connection object
    默认连接CFFEX_IF_Quote Database
    '''    
    conn = pymssql.connect(host,user,pwd,database)
    return conn

def error_NotImplemented(strFuncName):
    print(strFuncName + 'Not Implemented')

'''
class Instrument:
    def __init__(self):
        name = 'instrument'
        type = 'stock|futures|options'
        dailyTradingSessions = []


class HS300IF(Instrument):
    def __init__(self):
        Instrument.__init__()
        self.name = 'IF'
        dataMgr = IFDataManager()
        
'''
freq = Enum('BarFrequency','Snapshot Tick Min1 Min5 Min15 Min30 Hr1 Day1')

class Contract:
    def __init__(self):
        self.secID
        self.beginDate
        self.endDate
        self.instrument
        self.data = {}
    def __getitem__(self,freq_key):
        return self.data[freq_key]
    
    
        

class DataManager:
    '''
    同意管理所有sub data manager，所有sub manager需要有统一接口,统一名称，统一格式
    实现代码最大重用
    
    #reference to data
    data['IF1005'][freq.Min1]    
    
    data.load('IF1005',freq.Min1)
    data.update("CFFEX_IF",dt.date(2010,5,1))
    data.getActiveContracts("CFFEX_IF",dt.date(2010,5,1))
    data.getSettleDate("IF1005")
    data['IF1005'][freq.Min1].beginD
    
    对索引进行重载
    实现一个data类管理所有数据
    
    '''
    def __init__(self):
        self.managers = {}
        self.managers['CFFEX_IF'] = IFDataManager()
        self.contracts = {}
    
    def __getitem__(self,key):
        return self.contracts[key]
    
    def contract2Instrument(self,str_SecID):
        pass
    
    def load(self,str_SecID,freq,start='',end=''):
        loader = self.managers[self.contract2Instrument(str_SecID)]
        self.contracts[str_SecID][freq] = loader.load(SecID, start, end)
    
    def getActiveContracts(self,str_instrument,dtdate_t):
        self.managers[str_instrument].getActiveContracts(dtdate_t)
    
    def getSettleDate(self,str_SecID):
        pass
        



 
class IFDataManager:
    def __init__(self):
        self.sqlDBName = 'CFFEX_IF_Quote'
        self.engine = sqlalchemy.create_engine('mssql+pymssql://sa:871108@localhost/' + self.sqlDBName)

    def getIFQuoteTable(self,tableName,start='',end=''):
        '''
            返回tick table from sql server
            @tableName:str,'IF1005',IF1006,...
            @start:str,'2005-06-01'
            @end:str,'2005-06-01'
        '''
        engine = sqlalchemy.create_engine('mssql+pymssql://sa:871108@localhost/CFFEX_IF_Quote')        
        queryWithDuration = 'select SecID,QuoteTime,LastPrice,CumVolume, CumAmount, OpenInterest, Bid1, Bid1Vol,Offer1,Offer1Vol from %s where QuoteTime between cast(\'%s\' as datetime2) and cast(\'%s\' as datetime2) order by QuoteTime' % (tableName,start,end)
        queryAll = 'select SecID,QuoteTime,LastPrice,CumVolume, CumAmount, OpenInterest, Bid1, Bid1Vol,Offer1,Offer1Vol from %s order by QuoteTime' % (tableName)       
        if start != '' and end != '':
            result = pd.read_sql_query(queryWithDuration, engine,parse_dates=['QuoteTime'],index_col='QuoteTime')
        else:
            result = pd.read_sql_query(queryAll, engine,parse_dates=['QuoteTime'],index_col='QuoteTime')
        return result

    
    def loadBarData(self,tableName,freq='Bar_1_Min'):
        '''
        1Min, 5Min, 15Min, 1Hr, Day, Snapshot, tick
        '''
        
        barDir = 'C:\database\CFFEX_IF_Bar'
        hdfAddr = barDir + '\\' + tableName + '.h5'
        if not os.path.exists(hdfAddr):
            return pd.DataFrame()
            
        store = pd.HDFStore(hdfAddr)
        if '/Bar_1_Min' in store.keys():
            return store['Bar_1_Min']
        else:
            store.close()
            return pd.DataFrame()
    
    def getMonths(self,tableName):
        '''
        列出tick表中所包含的月份
        @tableName:str,'IF1005'
        Return:        
        @months:list of str
        '''
        
        query = 'select max(convert(char(7),QuoteTime,120)) as Months from %s group by convert(char(7),QuoteTime,120) order by convert(char(7),QuoteTime,120)' % tableName
        result = pd.read_sql_query(query, self.engine)
        months = result.values.flatten().tolist()
        return months
    

    def mergeBar(self,df_newData,df_oldData,ndarray_oldDataTimeIndex,dttime_T,b_toBack):
        '''
        将老数据T时刻数据与T+/-1Min数据进行合并
        @dttime_T:dt.datetime,待操作的老数据时间点T
        @b_toBack: bool, 如果为True，将老数据T的值赋给新数据的T+1Min
                         如果为False，将老数据T的值赋给新数据的T-1Min
        '''
        #先找出老数据所有T时刻数据              
        timeIdxT = (ndarray_oldDataTimeIndex == dttime_T)
        dataT = df_oldData.ix[timeIdxT]
        if len(dataT) == 0:
            return df_newData
            
        
        for timestampT, value in dataT.iterrows():
            #如果老数据T时刻为nan，则不更新新数据                  
            if np.isnan(value.Close):
                continue
            
            #如果老数据T时刻有值而新数据T+/-1Min无值，则需要将T数据全部赋给T+/-1Min
            if b_toBack:                    
                Tshift1Min = timestampT + dt.timedelta(minutes=1)    
            else:
                Tshift1Min = timestampT - dt.timedelta(minutes=1)       
            if np.isnan(df_newData.ix[Tshift1Min].Close):
                df_newData.ix[Tshift1Min] = value
            
            #如果T与T+/-1Min均有值，则对T+/-1Min进行更新
            if value.High > df_newData.ix[Tshift1Min].High:
                df_newData.ix[Tshift1Min,'High'] = value.High
            if value.Low < df_newData.ix[Tshift1Min].Low:
                df_newData.ix[Tshift1Min,'Low'] = value.Low
            df_newData.ix[Tshift1Min,'Volume'] += value.Volume
            df_newData.ix[Tshift1Min,'Amount'] += value.Amount
            
            if b_toBack:
                df_newData.ix[Tshift1Min,'Open'] = value.Open
            else:
                df_newData.ix[Tshift1Min,'Close'] = value.Close
                df_newData.ix[Tshift1Min,'OpenInterest'] = value.OpenInterest
        return df_newData

    def transformToRegular1MinBar(self,Min_1_Data):
        '''
        对已有1min bar data进行修正
        先用index确保每个交易日每分钟的的bar都存在，即使没有数据
        9:14数据归到9:15内
        11:30数据归到11:29内
        15:15数据归到15:14内
        确保每个minute bar都存在
        '''
        
        #get all trading days from the old 1 min table
        days = np.unique(Min_1_Data.index.date)
        if len(days) == 0:
            return
            
        #形成每天9:15-15:14的index
        dayStr = days[0].strftime('%Y-%m-%d')
        morningBeginStr = dayStr + ' 09:15:00'
        morningEndStr = dayStr + ' 11:29:00'
        noonBeginStr = dayStr + ' 13:00:00'
        noonEndStr = dayStr + ' 15:14:00'
        dayIdx = pd.date_range(morningBeginStr,morningEndStr,freq='1Min') + pd.date_range(noonBeginStr,noonEndStr,freq='1Min')
        days = np.delete(days,0)
        for day in days:
            dayStr = day.strftime('%Y-%m-%d')
            morningBeginStr = dayStr + ' 09:15:00'
            morningEndStr = dayStr + ' 11:29:00'
            noonBeginStr = dayStr + ' 13:00:00'
            noonEndStr = dayStr + ' 15:14:00'
            dayIdx = dayIdx + pd.date_range(morningBeginStr,morningEndStr,freq='1Min') + pd.date_range(noonBeginStr,noonEndStr,freq='1Min')
            
        #extract data from old 1 Min data
        newData = Min_1_Data.ix[dayIdx]
        
        #处理9:14,11:30以及15:15的数据
        timeIdx = Min_1_Data.index.time
        newData = self.mergeBar(newData,Min_1_Data,timeIdx,dt.time(9,14),True)
        newData = self.mergeBar(newData,Min_1_Data,timeIdx,dt.time(11,30),False)
        newData = self.mergeBar(newData,Min_1_Data,timeIdx,dt.time(15,15),False)
        return newData
  
    def regenerate1MinTable(self,tableName):
        
        months = self.getMonths(tableName)
        if len(months) == 0:
            return
        
        barDir = 'C:\database\CFFEX_IF_Bar'
        hdfAddr = barDir + '\\' + tableName + '.h5'
        store = pd.HDFStore(hdfAddr)
        if '/Bar_1_Min' in store.keys():
            del store['Bar_1_Min']
        
        
        for mon in months:
            #year,mon = temp.split('-')
            tempDate = dt.datetime.strptime(mon,'%Y-%m')
            nextMon = tempDate + relativedelta.relativedelta(months=1)
            start = tempDate.strftime('%Y-%m-%d %H:%M:%S')
            end = nextMon.strftime('%Y-%m-%d %H:%M:%S')
            
            result = self.getIFQuoteTable(tableName,start=start,end=end)
            
            #resample生成1min Bar
            ohlc = result['LastPrice'].resample('1Min',how='ohlc',closed='left')
            volAmtOi = result[['CumVolume','CumAmount','OpenInterest']].resample('1Min',how='last',closed = 'left')
            ohlc = ohlc.dropna()
            volAmtOi = volAmtOi.dropna()
            
            #删除原tick文件节省内存
            del result
            
            # 去除9:14:00以前及15:15:05以后的数据
            timeIdx = ohlc.index.time
            timeIdx = ((timeIdx >= dt.time(9,14,0)) & (timeIdx <=dt.time(11,30,5))) | ((timeIdx >= dt.time(13,0,0)) & (timeIdx <=dt.time(15,15,5)))
            ohlc = ohlc[timeIdx]
            volAmtOi = volAmtOi[timeIdx]
            
            #将累计成交量与累计成交额转化为成交量、成交额
            prevDate = volAmtOi.index[0].date() - dt.timedelta(days=1)
            prevCumVol = 0.0
            prevCumAmt = 0.0
            for i in range(len(volAmtOi)):
                currentDate = volAmtOi.index[i].date()
                #如果是每日首日bar，不进行转换
                if currentDate != prevDate:
                    prevDate=currentDate
                    prevCumVol = volAmtOi.iloc[i]['CumVolume']
                    prevCumAmt = volAmtOi.iloc[i]['CumAmount']
                    continue
                
                currentCumVol = volAmtOi.iloc[i]['CumVolume']
                currentCumAmt = volAmtOi.iloc[i]['CumAmount']
                volAmtOi.ix[i,'CumVolume'] = currentCumVol - prevCumVol
                volAmtOi.ix[i,'CumAmount'] = currentCumAmt - prevCumAmt
                prevCumVol = currentCumVol
                prevCumAmt = currentCumAmt
            
            #生成新表,append到hdf5中
            result = pd.merge(ohlc,volAmtOi,left_index=True,right_index=True)
            result.columns=[['Open', 'High', 'Low', 'Close', 'Volume', 'Amount','OpenInterest']]
            result['SecID'] = tableName
            
            #Transform irregular 1 min bar data to regular bar data
            result = self.transformToRegular1MinBar(result)
            store.append('Bar_1_Min', result)
        store.close()
        
    def getSettleDate(str_SecID):
        '''
        返回一个IF合约的交割日
        Returns
        -------
        result: dt.datetime
        '''
        
        conn = getMSSQLConnection()
        cursor = conn.cursor()
        query = 'select SettleDate from IF_Settle_Date where SecID = \'%s\' ' % (str_SecID)
        return cursor.execute(query).fetchone()
        
    def getActiveContracts(self,dt_t):
        conn = getMSSQLConnection()
        cursor = conn.cursor()        
        query = 'select secID from IF_Settle_Date where SettleDate >= cast(\'%s\' as datetime) order by SettleDate' % dt_t.strftime('%Y-%m-%d');
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result) == 0:
            return []
        
        secID = result[0][0]
        currentMon = dt.datetime.strptime(secID[2:],'%y%m')
        nextMon = currentMon + relativedelta.relativedelta(months=1)
        
        if nextMon.month >= 1 and nextMon.month <= 2:
            nextQtr = dt.datetime(nextMon.year, 3, 1);
            next2Qtr = dt.datetime(nextMon.year, 6, 1);
        elif nextMon.month >= 3 and nextMon.month <= 5:
            nextQtr = dt.datetime(nextMon.year, 6, 1);
            next2Qtr = dt.datetime(nextMon.year, 9, 1);
        elif (nextMon.month >= 6 and nextMon.month <= 8):
            nextQtr = dt.datetime(nextMon.year, 9, 1);
            next2Qtr = dt.datetime(nextMon.year, 12, 1);
        elif (nextMon.month >= 9 and nextMon.month <= 11):
            nextQtr = dt.datetime(nextMon.year, 12, 1);
            next2Qtr = dt.datetime(nextMon.year+1, 3, 1);
        elif (nextMon.month == 12):
            nextQtr = dt.datetime(nextMon.year+1, 3, 1);
            next2Qtr = dt.datetime(nextMon.year + 1, 6, 1);
        
        cursor.close()
        conn.close()
        return [secID,'IF'+nextMon.strftime('%y%m'),'IF'+nextQtr.strftime('%y%m'),'IF'+next2Qtr.strftime('%y%m')]
    
    def __insertQuote(self, csvAddr, tableName, isCTP=False):
        '''
        把每日的csv形式的tick数据导入sql server数据库
        '''
        conn = getMSSQLConnection()
        cursor = conn.cursor()
        result = 1
        msg = 'Succesfully insert %s into %s' % (csvAddr,tableName)
        try:
            if isCTP:
                cursor.callproc('pr_bulk_Insert_IF_Quote_From_KenSuo_CTP',(csvAddr,tableName))
            else:
                cursor.callproc('pr_bulk_Insert_IF_Quote_From_KenSuo_NonCTP',(csvAddr,tableName))
        except Exception as err:
            result = 0
            msg = err.__str__()
            cursor.close()
            conn.close()
        return (result,msg)
    
        
    def __getCsvAddr(self,str_contract,dtdate_t):
        '''
        返回指定合约str_Contract的日期dtdate_t的tick文件存储地址
        '''
        return r'C:\database\raw\CFFEX_IF_Quote\Kensuo\IF_'+dtdate_t.strftime('%Y%m') + '\\' + str_contract + '_' + dtdate_t.strftime('%Y%m%d')+'.csv'
    
    def dailyUpdate(self,dtdate_t):
        
        #将csv数据插入sql server
        activeContracts = self.getActiveContracts(dtdate_t)
        updateList = []
        for contract in activeContracts:
            checkResult = '未检测到:'
            csvAddr = self.__getCsvAddr(contract,dtdate_t)
            if os.path.exists(csvAddr):
                updateList.append(contract)
                checkResult = '检测到'
            print(contract + '  ' + checkResult + '  ' + csvAddr)
        
        if len(updateList) == 0:
            print('未检测到任何文件，停止更新')
            return
        
        print('继续更新已有源文件合约请按Y')
        letUpdate = input()
        if letUpdate == 'Y':
            for contract in updateList:
                updateResult = self.__insertQuote(self.__getCsvAddr(contract,dtdate_t),contract,isCTP=False)
                print(contract + '  ' + updateResult[1])
        else:
            print('不更新，结束')
        
        #提取当天的tick数据，将tick数据转为regular 1min bar数据，插入1 min bar当中
        #根据1min bar数据，形成5min,15min,1hr,daily bar,插入相应表中
        #插入应该是replace操作：delete old,insert new, sort，保证数据无重复
        #记得还要regenerate 5min 15min 1hr daily bar table，对历史数据
            
            
            
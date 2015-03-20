# -*- coding: utf-8 -*-
"""
Created on Sun Mar  1 02:09:49 2015

@author: Brandon
"""
import datetime as dt
import ry_database as rydb
import pandas as pd
from dateutil.relativedelta import relativedelta
from imp import reload
reload(rydb)
ifmgr = rydb.IFDataManager()

beginDate = dt.datetime(2015,3,1)
t = beginDate
endDate = dt.datetime(2015,12,1)

while t<=endDate:
    tableName = 'IF' + t.strftime('%y%m')
    ifmgr.regenerate1MinTable(tableName)
    print('Complete ',tableName)
    t = t + relativedelta(months=1)
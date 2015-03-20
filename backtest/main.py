# -*- coding: utf-8 -*-
"""
Created on Thu Mar  5 10:33:24 2015

@author: Brandon
"""

import datetime as dt
import pandas as pd
import numpy as np
import ry_database as rydb

ifmgr = rydb.IFDataManager()
ifmgr.dailyUpdate(dt.date(2015,3,11))

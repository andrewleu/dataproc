#!/bin/python
# -*- coding: UTF-8 -*-
import sys
import MySQLdb as mysql
import os
import zipfile
from datetime import *
import time 
path='/home/rsync/files'
insertpath='/home/'
#dbaddr="172.24.20.68"
dbaddr="127.0.0.1"
#if len(args) <2 :
tab=mysql.connect(dbaddr,'ipv6bgp','ipv6','data',charset='utf8')
cur_tab=tab.cursor();
cur_tab.execute("set names 'utf8'")
reload(sys)
sys.setdefaultencoding('UTF-8')
line=cur_tab.execute("select  distinct left(timestamp, 10) from streaming where YM=0 and name='Tencent' ")
ymd=list(cur_tab.fetchall())
i=0
length=ymd.__len__()
while i<length :
    ymdstr=ymd[i][0].replace('-','').replace(' ','').replace(':','')
    ym= ymdstr[0:6]
    day=ymdstr[6:8]
    cur_tab.execute("update streaming set YM=%s, day=%s where timestamp like '%s' and YM=0 and name='Tencent'" \
     % (ym,day,ymd[i][0]+'%'))
    cur_tab.execute("update streaming set status='b' where substring(timestamp,12,2) in (19,20,21,22) and name='Tencent' \
    and YM=%s and day=%s and status='n' " % (ym,day))
    cur_tab.execute("update streaming set status='i' where substring(timestamp,12,2) in (01,02,03,04,05) and name='Tencent'\
    and YM=%s and day=%s and status='n' " % (ym,day))
    cur_tab.execute("update streaming set status='o' where YM=%s and day=%s and name='Tencent' and status='n' " % (ym,day)) 
    cur_tab.execute("commit")
    i=i+1
cur_tab.close()
tab.close()    
       



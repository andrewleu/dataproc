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
while 1 :
    try: 
       line=cur_tab.execute("select id, timestamp from streaming where YM=0 and name='Tencent' limit 1")
       if line==0:
             break
    except  mysql.Error, e:
           print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
           continue    
    ymd=list(cur_tab.fetchone())
    ymd[1]=ymd[1].replace('-','').replace(' ','').replace(':','')
    ym= ymd[1][0:6]
    day=ymd[1][6:8];hour=int(ymd[1][8:10])
    try :
       cur_tab.execute("select id, timestamp from streaming where id=%s for update" % ymd[0])
       cur_tab.execute("update streaming set YM='%s', day='%s' where id='%s' \
       " % (ym, day, ymd[0]))
       if hour>=19 and hour<=22 :
         cur_tab.execute ("update streaming set status='b' where id='%s' " % ymd[0])
       elif hour>=1 and hour<=5 :
         cur_tab.execute ("update streaming set status='i' where id='%s' " % ymd[0])
       else :
         cur_tab.execute("update streaming set status='o' where id='%s'" % ymd[0])
       cur_tab.execute("commit")
    except  mysql.Error, e:
           print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
           continue
cur_tab.close()
tab.close()    
       



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
tab=mysql.connect(dbaddr,'root','rtnet','data',charset='utf8')
cur_tab=tab.cursor();
cur_tab.execute("set names 'utf8'")
reload(sys)
sys.setdefaultencoding('UTF-8')
cur_tab.execute(" select curdate()")
#mysql is very good tool to calculate date before and after nowaday
initdate=cur_tab.fetchone()
yyyymmdd=str(initdate[0].isoformat())
# format the datetime.date to string in iso formation like yyyy-mm-dd
ym=yyyymmdd.replace('-','').replace(' ','')[0:6]
yyyymmdd=yyyymmdd[0:7]
#string just substracted to yyyy-mm, and replace blank and useless char
print ym, yyyymmdd
cur_tab.execute ("select entry_num, `lines` from dlfiles where id= (select min(id) from dlfiles where filename like 'chinacache%%' \
and endtime like '%s%%') " % yyyymmdd)
# to escape char % you must put another % ahead
# entry_num substracting lines is the first entry_id of the month
dlline=cur_tab.fetchone();
if dlline[0] >=dlline[1] :
 dlline=dlline[0]-dlline[1]
else :
 dlline= dlline[0]
# chinacache the minimum line No.
try :
   cur_tab.execute("ALTER  VIEW \
   dl_ent AS select download.id AS id,download.ipaddr AS ipaddr,download.addr AS addr, \
   download.addr_2 AS addr_2,download.addr_3 AS addr_3,download.agency AS agency,\
   download.date AS date,download.avi AS avi,download.max AS max, \
   download.purpose AS purpose,download.status AS status,download.YM AS YM,\
   download.day AS day from download \
   WHERE ((download.YM = %s) AND ((download.status = 'i') OR \
   (download.status = 'b')) and id>=%s)" % (ym, dlline))
except  mysql.Error, e:
     expt=1
     print "downloading"
     print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
cur_tab.execute("commit")
cur_tab.close()
tab.close()    
       



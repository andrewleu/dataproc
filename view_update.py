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
cur_tab.execute(" select date_add(curdate(), interval -1 month)")
initdate=cur_tab.fetchone()
ym=str(initdate[0].isoformat())
ym=ym.replace('-','').replace(' ','')[0:6]
print ym
try :
   cur_tab.execute("ALTER  VIEW \
   dl AS select download.id AS id,download.ipaddr AS ipaddr,download.addr AS addr, \
   download.addr_2 AS addr_2,download.addr_3 AS addr_3,download.agency AS agency,\
   download.date AS date,download.avi AS avi,download.max AS max, \
   download.purpose AS purpose,download.status AS status,download.YM AS YM,\
   download.day AS day from download \
   WHERE ((download.YM = %s) AND ((download.status = 'i') OR \
   (download.status = 'b')))" % ym) 
   cur_tab.execute("ALTER  VIEW streaming1 AS select streaming.id AS id,\
   streaming.name AS name,streaming.ipaddr AS ipaddr,streaming.province AS province,\
   streaming.city AS city,streaming.county AS county,streaming.carrier AS carrier,\
   streaming.timestamp AS timestamp,streaming.os AS os,streaming.term AS term,\
   streaming.software AS software,streaming.s_rate AS s_rate,\
   streaming.s_duration AS s_duration,streaming.watching AS watching,\
   streaming.download AS download,streaming.buffer AS buffer,\
   streaming.buffer_duration AS buffer_duration,streaming.YM AS YM,streaming.day AS day,\
   streaming.status AS status from streaming WHERE ((streaming.YM = %s) AND \
   ((streaming.status = 'i') OR (streaming.status = 'b'))) " % ym) 
except  mysql.Error, e:
     expt=1
     print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
cur_tab.execute("commit")
cur_tab.close()
tab.close()    
       



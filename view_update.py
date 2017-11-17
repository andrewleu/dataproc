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
cur_tab.execute ("select entry_num, `lines` from dlfiles where id= (select min(id) from dlfiles where (filename like 'youku%%' or filename regexp '^tencent.*zip$') \
and endtime like '%s%%') " \
% yyyymmdd)
#use regexp ^ following the starting string
stline=cur_tab.fetchone()
if stline[0] >= stline[1] :
   stline=stline[0]-stline[1]
else :
   stline=stline[0]
#first line of the streaming of the month
try :
   cur_tab.execute("ALTER  VIEW \
   dl AS select download.id AS id,download.ipaddr AS ipaddr,download.addr AS addr, \
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
try: 
   cur_tab.execute("ALTER  VIEW streaming1 AS select streaming.id AS id,\
   streaming.name AS name,streaming.ipaddr AS ipaddr,streaming.province AS province,\
   streaming.city AS city,streaming.county AS county,streaming.carrier AS carrier,\
   streaming.timestamp AS timestamp,streaming.os AS os,streaming.term AS term,\
   streaming.software AS software,streaming.s_rate AS s_rate,\
   streaming.s_duration AS s_duration,streaming.watching AS watching,\
   streaming.download AS download,streaming.buffer AS buffer,\
   streaming.buffer_duration AS buffer_duration,streaming.YM AS YM,streaming.day AS day,\
   streaming.status AS status from streaming WHERE ((streaming.YM = %s) AND \
   ((streaming.status = 'i') OR (streaming.status = 'b')) and id>= %s) " % (ym, stline)) 
except  mysql.Error, e:
     expt=1
     print "streaming"
     print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
date=str(initdate[0].isoformat())[0:7]
if cur_tab.execute("select min(line) from webfiles where starttime like '%s%%'" % date) :
   line=cur_tab.fetchone()[0]
   cur_tab.execute("ALTER  VIEW web as select `webbrowsing`.`id` AS `id`,`webbrowsing`.`name` AS `name`,  \
   `webbrowsing`.`ip` AS `ip`,`webbrowsing`.`province` AS `province`,`webbrowsing`.`city` AS `city`, \
   `webbrowsing`.`county` AS `county`, `webbrowsing`.`carrier` AS `carrier`,`webbrowsing`.`date` AS `date`, \
   `webbrowsing`.`os` AS `os`,`webbrowsing`.`tcp` AS `tcp`,`webbrowsing`.`terminal` AS `terminal`, \
   `webbrowsing`.`browser` AS `browser`, `webbrowsing`.`url` AS `url`,`webbrowsing`.`1stscreen` AS `1stscreen`,\
   `webbrowsing`.`1stpage` AS `1stpage`, `webbrowsing`.`pagesize` AS `pagesize`,\
   `webbrowsing`.`domready` AS `domready`,`webbrowsing`.`prtcol`  \
   AS `prtcol`,`webbrowsing`.`status` AS `status` from `webbrowsing`  \
   where (`webbrowsing`.`prtcol` = '%s') and id >%s-40000" % (date.replace('-',''), line))
cur_tab.execute("commit")
cur_tab.close()
tab.close()    
       



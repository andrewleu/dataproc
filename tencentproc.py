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
qstr="201600"
#if len(args) <2 :
tab=mysql.connect(dbaddr,'ipv6bgp','ipv6','data',charset='utf8')
cur_tab=tab.cursor();
cur_tab.execute("set names 'utf8'")
reload(sys)
sys.setdefaultencoding('UTF-8')
cur_tab.execute("select min(id) from streaming where ym=%s and name='tencent' and id>520000000" % qstr)
min_id=cur_tab.fetchone()
min_id=min_id[0]
if min_id!=None :
  start_id=min_id;#start i
  while cur_tab.execute("select id, timestamp from streaming where YM=%s and name<>'ku6' and id >= %s order by rand() limit 500  " % (qstr, min_id)) : 
    ymd=list(cur_tab.fetchall()) ; 
    counter=0;
    excpt=0  
    while counter<ymd.__len__() :
      yyyymmdd=ymd[counter][1].replace('-','').replace(' ','').replace(':','')
      id=ymd[counter][0]
      ym= yyyymmdd[0:6]; 
      day=yyyymmdd[6:8];
      hour=int(yyyymmdd[8:10])
      counter=counter+1 
      try :
         cur_tab.execute("select id, timestamp from streaming where id=%s for update" % id)
      except  mysql.Error, e:
           print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
           continue
      try :
        cur_tab.execute("update streaming set YM='%s', day='%s' where id='%s' \
        " % (ym, day, id))
        if hour>=19 and hour<=22 :
           cur_tab.execute ("update streaming set status='b' where id='%s' " % id)
        elif hour>=1 and hour<=5 :
           cur_tab.execute ("update streaming set status='i' where id='%s' " % id) 
        else :
          cur_tab.execute ("update streaming set status='o' where id='%s' " % id)
      except :
         excpt=1
         cur_tab.execute("commit")
      if (counter %10000)==0 :
          print counter-1, yyyymmdd , id
          cur_tab.execute("commit")
      if excpt :
         break
    cur_tab.execute("commit")
'''
if min_id!=None : 
  cur_tab.execute("select  distinct left(timestamp, 8)  from streaming where  YM=0  and id>= %s " % (min_id))
  ymd=list(cur_tab.fetchall())
  i=0
  length=ymd.__len__(); print ymd;  
  #total items in the result
  while i<length :
    if ymd[i][0] == None :
      i=i+1
      continue
    ymdstr=ymd[i][0].replace('-','').replace(' ','').replace(':',''); print ymdstr
    if len(ymdstr)< 8 :
      i=i+1
      continue
    ym= ymdstr[0:6]
    day=ymdstr[6:8];
    if day == None :
      day='00'
    print "the date under processing: %s" % ymd[i][0]
    cur_tab.execute("update streaming set YM=%s, day=%s where timestamp like '%s' and YM=0  and id >= %s" \
    % (ym,day,ymd[i][0]+'%',min_id))
    i=i+1
    cur_tab.execute("commit")
    cur_tab.execute("update streaming set status='b' where substring(timestamp,12,2) in (19,20,21,22) and status='n'  and ym=%s and day=%s and id >=%s" % (ym,day,min_id) )
    cur_tab.execute("update streaming set status='i' where substring(timestamp,12,2) in (01,02,03,04,05) and status='n'  and ym=%s and day=%s and id>=%s" % (ym,day, min_id) )
    cur_tab.execute("update streaming set status='o' where   status='n' and ym=%s and day=%s" % (ym,day) )
    cur_tab.execute("commit")
    cur_tab.execute("select min(id) from streaming where ym=0")
    min_id=cur_tab.fetchone()
    min_id=min_id[0]
    # minmun id updated, since more entries updated 
if start_id!=None :
   cur_tab.execute("update streaming set ym=201600 where ym=0 and id>= %s" % (start_id))
   cur_tab.execute("update streaming set buffer_duration=buffer, buffer=download, download=watching, watching=s_duration \
   where buffer_duration is NULL and name='Tencent' and id>=%s" % (start_id))
'''
cur_tab.execute("commit")
cur_tab.close()
tab.close()    
       



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
cur_tab.execute("select filename from dlfiles where id= (select max(id) from dlfiles where filename regexp '^tencent' and filename regexp 'zip$')")
zp=cur_tab.fetchone()[0]+'\n';print zp #last zip file
os.chdir('%s' % (path)) #change the dir to the one which contains the files
ziplist=(os.popen('ls tencent*.zip').readlines())
listno=ziplist.index(zp) # how many files tencent has uploaded
print "Tencent Downloading Data"
listno=listno+1
print zp  
# select the starting point, keep the program from scanning the whole table
cur_tab.execute("select min(id) from streaming where ym=0")
min_id=cur_tab.fetchone()
min_id=min_id[0]
if min_id==None :
   cur_tab.execute("select max(id) from streaming")
   min_id=cur_tab.fetchone()
   min_id=min_id[0]
start_id=min_id;#start id 
#select the starting point, keep the program from scanning the whole table
while listno<len(ziplist) :
  zipf=zipfile.ZipFile(ziplist[listno][0:len(ziplist[listno])-1],'r')
  print zipf.namelist()
  files=0; lines=0; inittime=datetime.now()
  for filename in zipf.namelist():
        print filename
  	zipf.extract(filename,insertpath); pfilename=insertpath+filename;expt=0
        os.system("sed -i 1d %s" % (pfilename)) #delete firstline
	os.system("sed -i '/,,[0-9]*,[0-9]*/d' %s" % (pfilename)) 
		#delete first line and those lines whose download field is null
	try:
	    cur_tab.execute("load data local infile '%s' into table streaming Fields Terminated By ','  \
	    ( name,ipaddr,  province,city,county, carrier, timestamp, os, term,software, `s_rate`, \
	    `s_duration`, watching,download, buffer, `buffer_duration`)" % (pfilename)) 
     #new uploaded file have county lines, so I have to insert the field also
	except  mysql.Error, e:
           expt=1;print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
	finally :
        	cur_tab.execute("commit")
	if expt==0:
            newlines=os.popen("wc -l %s" % (pfilename)).readline()
    	    newlines=newlines.split()
    	    lines=lines+int(newlines[0])
	    os.system('rm -f %s' % (pfilename))
  cur_tab.execute("insert into dlfiles (filename, starttime, endtime,`lines`) \
  values('%s','%s',now(),%s) " % (ziplist[listno][0:len(ziplist[listno])-1], inittime,lines))
  cur_tab.execute("commit")
  zipf.close() ; 
  listno=listno+1
  while cur_tab.execute("select id, timestamp from streaming where YM=201600 and name='Tencent' and id >= %s  order by rand() limit 1" % (start_id)) :
    ymd=list(cur_tab.fetchone()) ; #print ymd
    ymd[1]=ymd[1].replace('-','').replace(' ','').replace(':','')
    ym= ymd[1][0:6]; 
    day=ymd[1][6:8];hour=int(ymd[1][8:10])
    try :
       cur_tab.execute("select id, timestamp from streaming where id=%s for update" % ymd[0])
    except  mysql.Error, e:
           print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
           continue
    cur_tab.execute("update streaming set YM='%s', day='%s' where id='%s' \
    " % (ym, day, ymd[0]))
    if hour>=19 and hour<=22 :
       cur_tab.execute ("update streaming set status='b' where id='%s' " % ymd[0])
    elif hour>=1 and hour<=5 :
      cur_tab.execute ("update streaming set status='i' where id='%s' " % ymd[0]) 
    else :
      cur_tab.execute ("update streaming set status='o' where id='%s' " % ymd[0])
    cur_tab.execute("commit")
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
cur_tab.execute("commit")
cur_tab.close()
tab.close()    
       



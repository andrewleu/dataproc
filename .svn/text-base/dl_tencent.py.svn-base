#!/bin/python
# -*- coding: UTF-8 -*-

import sys
import MySQLdb as mysql
import os
import zipfile
from datetime import *
import time 
path='/home/rsync/files'
insertpath='/home/mysql/data/'
dbaddr="127.0.0.1"
#if len(args) <2 :
tab=mysql.connect(dbaddr,'ipv6bgp','ipv6','data',charset='utf8')
cur_tab=tab.cursor();
cur_tab.execute("set names 'utf8'")
reload(sys)
sys.setdefaultencoding('UTF-8')
cur_tab.execute("select filename from dlfiles where id= (select max(id) from dlfiles where filename regexp '^tencent' and filename regexp 'zip$')")
zp=cur_tab.fetchone()[0]+'\n';print zp
os.chdir('%s' % (path)) #change the dir to the one which contains the files
ziplist=(os.popen('ls tencent*.zip').readlines())
listno=ziplist.index(zp)
listno=listno+1
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
		    cur_tab.execute("load data infile '%s' into table streaming Fields Terminated By ','  \
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
  zipf.close()
  cur_tab.execute("select distinct left(timestamp,8) from streaming where YM=0")
  ymd=cur_tab.fetchall()
  listlen=len(ymd);i=0
  while i<listlen :
  	  ym= ymd[i][0][0:6]
  	  day=ymd[i][0][6:8]
  	  cur_tab.execute("update streaming set YM='%s', day='%s' where timestamp like '%s'\
  	  and YM=0" % (ym, day, ymd[i][0]+'%'))
  	  cur_tab.execute("update streaming set status='b' where substring(timestamp,9,2) in (19,20,21,22) and \
  	  YM='%s' and day='%s' 	and status='n'" % (ym, day))
  	  cur_tab.execute("update streaming set status='i' where substring(timestamp,9,2) in (01,02,03,04,05) and \
  	  YM='%s' and day='%s' 	and status='n'" % (ym, day))
  	  i=i+1
  	  cur_tab.execute("commit")
  listno=listno+1 
  	 	
cur_tab.close()
tab.close()    
       



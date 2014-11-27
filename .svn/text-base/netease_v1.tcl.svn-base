#!/usr/bin/tclsh
package require mysqltcl
global mysqlstatus

set port {3306}
set host {127.0.0.1}
set user {ipv6bgp}
set password {ipv6}
set dbname {data}
set unzipfile /home/rsync/temp
set insertfile /var/lib/mysql/data/temp1
cd /home/rsync
set flist [lsort [glob netease*]]
set mysql_handler [mysqlconnect -host $host -port $port -user $user -password $password]
mysqluse $mysql_handler $dbname
set lastfile [mysqlsel $mysql_handler "select id, filename from webfiles where  id = (select max(id) from webfiles where filename like 'netease%') " -list]
set lastfile [lindex $lastfile 0 1]
puts $lastfile
set n_file [lsearch $flist $lastfile]
incr n_file
set init_time [clock seconds]
set init_time [clock format $init_time -format  {%Y-%m-%d %H:%M:%S}] 
# start time 
while { [set read_file [lindex $flist $n_file]]!=""} {
   puts $read_file
   exec gzip -dc $read_file > $unzipfile
   set fp [open $unzipfile r]
   set tf [open $insertfile w]
#fconfigure $fp -encoding gb2312
   gets $fp msg
   set line 1
   while { $msg != ""} {
     set num 0
     set count 1
     set chrct [string index $msg $num  ]
     while {$chrct !=""} {
     if {$chrct =="/"} {
          set msg [string replace $msg $num $num ","]
          incr count
          if {$count>2} { break }
          }
    
     incr num
     set chrct [string index $msg $num  ]
     }
   puts $tf $msg
   gets $fp msg
   incr line
#   puts $line
  }
   close $fp
   close $tf
  mysqlexec $mysql_handler "load data  infile '$insertfile'  ignore into table webbrowsing Fields Terminated By ',' 
   (name,     ip,province,
    city,county,carrier,date, os,tcp,terminal,browser,url,1stscreen,1stpage,pagesize,domready)"
   mysqlexec $mysql_handler "insert into webfiles (filename,starttime, endtime) values ('$read_file','$init_time',now())"

   incr n_file
}
mysqlclose $mysql_handler   

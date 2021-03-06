#!/usr/bin/tclsh
package require mysqltcl
global mysqlstatus
set port {3306}
#set host {172.24.20.68}
set host {127.0.0.1}
set purpose "youku"
set user {ipv6bgp}
set password {ipv6}
set dbname {data}
set dltbl "dlfiles"
#set unzipfile /home/rsync/dltemp
set insertfile /home/tempyouku
#have to be this dir
cd /home/rsync/files
set flist [lsort [glob $purpose*]]
#start time
set mysql_handler [mysqlconnect -host $host -port $port -user $user -password $password]
mysqluse $mysql_handler $dbname
set var '$purpose%'
set lastfile [mysqlsel $mysql_handler "select id, filename from $dltbl where id = (select max(id) from $dltbl where filename like $var)" -list]
set lastfile [lindex $lastfile 0 1]
set n_file [lsearch $flist $lastfile]
incr n_file
set line 0
while { [set read_file [lindex $flist $n_file]]!=""} {
    puts $read_file
    #set read_file "chinacache_20130501205026.txt"
    if {[string match *gz $read_file]!= 0 } {
            set tempfilename $read_file
            exec gzip  -dc $read_file > $insertfile
#            set read_file $insertfile 
           #set fp [open $read_file r]
            set zipped 1
          } else {
             #     set fp [open $read_file r]
                  set zipped 0
                  set insertfile $read_file
          } 
   set lines [exec wc -l $insertfile]
   puts $lines
   set lines [lindex $lines 0] 
   set init_time [clock seconds]
   set init_time [clock format $init_time -format  {%Y-%m-%d %H:%M:%S}]
   set error_code [catch { mysqlexec $mysql_handler "load data local infile '$insertfile'      into table streaming Fields Terminated By ',' ( name,ipaddr,  province,city,county, carrier, timestamp, os, term,software, `s_rate`,`s_duration`, watching,download, buffer, `buffer_duration`)"} msg]
   if {$error_code} {puts $msg; set lines 0;}
       # mysql 5.5 will not work with 'local' option 
   set entry_num [mysqlsel $mysql_handler "select max(id) from streaming   " -list]
   mysqlexec $mysql_handler "insert into dlfiles (filename, starttime, endtime,`lines`, `entry_num`)  values ('$read_file','$init_time', now(),$lines, $entry_num)"
   incr n_file
#   set ymd [mysqlsel $mysql_handler "select distinct left(timestamp, 8) 
#      from streaming where YM=0 and name<>'Tencent'" -list] 
#   set len [llength $ymd]
#   set i 0
#   while { $i < $len} {
#      set word [lindex $ymd  $i]
#      if { [string range $word 0 0] !="2"} {
#          incr i
#          continue }
#      set YM [string range $word 0 5]
#      set day [string range $word 6 7]
#      mysqlexec $mysql_handler "update streaming set YM=$YM, day=$day where timestamp
#          like '$word%' and YM=0 and name<>'Tencent'"
#      mysqlexec $mysql_handler "update streaming set status='b' where 
#          substring(timestamp,9,2) in (19,20,21,22)  and YM=$YM and day=$day and status='n'"
#      mysqlexec $mysql_handler "update streaming set status='i' where 
#          substring(timestamp,9,2) in (01,02,03,04,05) and YM=$YM and day=$day and status='n'"
#      mysqlexec $mysql_handler "update streaming set status='o' where
#           YM=$YM and day=$day and status='n'"
#      incr i
#   }
}

 mysqlclose $mysql_handler


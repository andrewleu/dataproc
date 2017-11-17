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
set ymd [mysqlsel $mysql_handler "select distinct left(timestamp, 10) 
      from streaming where YM=0 and name='Tencent'" -list] 
set len [llength $ymd]
set i 0
while { $i < $len} {
      set word [lindex $ymd  $i]
      if { [string range $word 0 0] !="2"} {
          continue }
      set YM [string range $word 0 5]
      set day [string range $word 6 7]
      mysqlexec $mysql_handler "update streaming set YM=$YM, day=$day where timestamp
          like '$word%' and YM=0 and name<>'Tencent'"
      mysqlexec $mysql_handler "update streaming set status='b' where 
          substring(timestamp,9,2) in (19,20,21,22)  and YM=$YM and day=$day and status='n'"
      mysqlexec $mysql_handler "update streaming set status='i' where 
          substring(timestamp,9,2) in (01,02,03,04,05) and YM=$YM and day=$day and status='n'"
      mysqlexec $mysql_handler "update streaming set status='o' where
           YM=$YM and day=$day and status='n'"
      incr i
   }
}

 mysqlclose $mysql_handler


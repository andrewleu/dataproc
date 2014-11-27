package require mysqltcl
global mysqlstatus
set port {3306}
set host {127.0.0.1}
set user {ipv6bgp}
set password {ipv6}
set dbname {data}
cd /home/rsync/files/
set flist [lsort [glob qihu*]]
set purpose "qihu"


# start time

set mysql_handler [mysqlconnect -host $host -port $port -user $user -password $password]
mysqlexec $mysql_handler "set names 'utf8'"
mysqluse $mysql_handler $dbname
set lastfile [mysqlsel $mysql_handler "select id, filename from dlfiles where id = (select max(id) from dlfiles where filename like 'qihu%')" -list]
set lastfile [lindex $lastfile 0 1]
set n_file [lsearch $flist $lastfile]
incr n_file

#set files [glob sohu*]
while { [set read_file [lindex $flist $n_file]]!=""} {
   puts $read_file
   set fp [open $read_file r]
   set line 0 
   set frgn "na"
   while  {[eof $fp] != 1} {
       gets $fp msg
       if {$msg == "" } {continue}
       set msg [string map {" " "_"} $msg] ;#no space # to check the field number
       if {$frgn == "na"} { ;#  first time read the file
            set location [string map {"," " "} $msg]  
            if { [lindex $location 6] != "" } {
                 set frgn 0 ;# for foreign 
                 set insertfile "/home/mysql/data/qhtemp"
                 set tf [open $insertfile w]
           
                 } else {
                 set frgn 1 ;# for domestic
                 set insertfile "/home/mysql/data/qhfrgntemp"
                 set tf [open $insertfile w]
        }
        puts $frgn
      }
        if {$frgn ==0 } {
         # use status to identify the hour of busy or idle
            set partition_col [string map {"," " "} $msg]
            set partition_col [lindex $partition_col 5] 
            set hours [string rang $partition_col 8 9]
            set day [string range $partition_col 6 7]
            set partition_col [string rang $partition_col 0 5]
            if { $hours >=19 && $hours < 23 }  {
                     set status "b"
            } elseif {
                   $hours >= 1 && $hours <6 }  {
                     set status "i"
           }  else {set status "o"}
          # end of hour identification
              #lappend msg ",$purpose,$status,$partition_col"
              set msg "$msg,qihu,$status,$partition_col,$day"
             puts $tf $msg
             if {[expr {$line%200000}] == 0 } {puts $line; puts $msg }
             incr line
             }
        if {$frgn ==1 } {
             set msg "$msg,qihu_foreign"
             puts $tf $msg
             #if {[expr {$line%1000}] == 0 } {puts $line $frgn }
             incr line
        }
        
   }    
            # next line edition

       close $fp
       close $tf
       set init_time [clock seconds]
       set init_time [clock format $init_time -format  {%Y-%m-%d %H:%M:%S}]
    if { $frgn != "na" } {
         if { $frgn ==0 } { 
          if { [catch {mysqlexec $mysql_handler "load data  infile '$insertfile' into table download Fields Terminated By ',' ( ipaddr,  addr, addr_2, addr_3, agency,date, max, avi, purpose, status,YM,day)"}  error ]}  {
           puts "open $read_file with $error" 
      } else {
       mysqlexec $mysql_handler "insert into dlfiles (filename, starttime, endtime,`lines`) values ('$read_file','$init_time', now(),$line)" }
       incr n_file
      }
      if {$frgn  ==1 } {
         catch { mysqlexec $mysql_handler "load data  infile '$insertfile' into table download_foreign Fields Terminated By ',' (ipaddr,addr,date,max,avi,purpose)" } error
        mysqlexec $mysql_handler "insert into dlfiles (filename, starttime, endtime, `lines`) values ('$read_file','$init_time', now(),$line)"
       incr n_file
      }
  }    
} 
mysqlclose $mysql_handler
 


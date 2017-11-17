package require mysqltcl
global mysqlstatus
set port {3306}
set host {127.0.0.1}
set user {ipv6bgp}
set password {ipv6}
set dbname {data}
cd /home/rsync
set flist [lsort [glob qihu*]]


# start time

set mysql_handler [mysqlconnect -host $host -port $port -user $user -password $password]
mysqluse $mysql_handler $dbname
set lastfile [mysqlsel $mysql_handler "select id, filename from dlfiles where id = (select max(id) from dlfiles where filename like 'qihu%')" -list]
set lastfile [lindex $lastfile 0 1]
set n_file [lsearch $flist $lastfile]
incr n_file

#set files [glob sohu*]
while { [set read_file [lindex $flist $n_file]]!=""} {
   puts $read_file
   set fp [open $read_file r]
   set line 1
   set frgn "na"
   while  {[eof $fp] != 1} {
       gets $fp msg
       if {$msg == "" } {continue}
        ;# to check the field number
        if {$frgn == "na"} { ;#  first time read the file 
            set location [string map {"," " "} $msg]
            if { [lindex $location 6] != "" } { 
            	set frgn 0
            	set insertfile "/var/lib/mysql/data/qhtemp"
            	set tf [open $insertfile w]
            
            	} else {
            	set frgn 1
              set insertfile "/var/lib/mysql/data/qhfrgntemp"
            	set tf [open $insertfile w]
        }
        puts $frgn
      }
        if {$frgn ==0 } {
        	 set msg "$msg,qihu,w"
        	 puts $tf $msg
        	 #if {[expr {$line%1000}] == 0 } {puts $line frgn}
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
        mysqlexec $mysql_handler "load data  infile '$insertfile'into table download Fields Terminated By ',' ( ipaddr,  addr, addr_2, addr_3, agency,date, max, avi, purpose, status)"
       mysqlexec $mysql_handler "insert into dlfiles (filename, starttime, endtime) values ('$read_file','$init_time', now())"
       incr n_file
      }
      if {$frgn  ==1 } {
       	mysqlexec $mysql_handler "load data  infile '$insertfile'into table download_foreign Fields Terminated By ',' (ipaddr,addr,date,max,avi,purpose)"
        mysqlexec $mysql_handler "insert into dlfiles (filename, starttime, endtime) values ('$read_file','$init_time', now())"
       incr n_file
      }
  }     
}  
mysqlclose $mysql_handler
  



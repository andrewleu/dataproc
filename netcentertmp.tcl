package require mysqltcl
global mysqlstatus
set port {3306}
#set host {172.24.20.68}
set host {127.0.0.1}
set purpose "netcenter_201512"
set user {ipv6bgp}
set password {ipv6}
set dbname {data}
set dltbl {dlfiles}
set unzipfile /home/rsync/nctemp
set insertfile /home/dltemp2
#have to be this dir
cd /home/rsync/files/
set flist [lsort [glob $purpose*.new]]
set mysql_handler [mysqlconnect -host $host -port $port -user $user -password $password]
mysqluse $mysql_handler $dbname
mysqlexec $mysql_handler "set names 'utf8'"
set var "netcenter"
set n_file 0
incr n_file
while { [set read_file [lindex $flist $n_file]]!=""} {
   puts  $read_file
   if { [catch {exec zcat $read_file > $unzipfile} out]} {
     puts $out; set unzipfile $read_file
   } 
   set tf [open $insertfile w]
   set fp [open $unzipfile r]
 #  set read_file $unzipfile
   set zipped 1
   fconfigure $fp -encoding utf-8
#   fconfigure $fp -encoding gb2312
   set nodata 1
   set line 0
   set firstline 1
            # next line edition

   while {[eof $fp] != 1} {
           gets $fp msg
            #puts "a"
           if { $msg == "" } {
                continue }
           set msg [string trimleft $msg " "]
           if {$firstline ==1 } {
              if { [string index $msg 0] != "\u7f51"} {
                             close $fp
                             incr firstline
                             set fp [open $unzipfile r]
                             fconfigure $fp -encoding  gb2312
                             gets $fp msg
                             puts 'gb2312'
                             if { [string index $msg 0] != "\u7f51"} {
                                set nodata 1
                                break
                             }
                   } else {puts 'utf-8'; incr firstline} 
           }
           set nodata 0 
           if { [string length $msg] <20 } {continue }
           set msg [string map { "\u7f51\u5bbf," ""} $msg]
           set msg [string map { " " "_"} $msg]

            # puts "2"
           set partition_col [string map {"," " "} $msg]
           set partition_col [lindex $partition_col 5] 
           set hours [string rang $partition_col 8 9]
           set day [string range $partition_col 6 7]
           set partition_col [string rang $partition_col 0 5]
           if { $hours >=19 && $hours < 23 }  {
                     set status "b"
           } elseif {
             $hours >= 1 && $hours <6
           }  {
                     set status "i"
           } else {set status "o"}
            lappend msg ",$var,$status,$partition_col,$day,$read_file"
            set msg [string map { " " ""} $msg]
            #puts "replace"
      #      set msg [lappend msg $strtail]
            puts $tf $msg
            incr line
            if {[ expr {$line%200000}] ==0} { puts $line 
             puts $msg
          }
       }
       
       close $fp
       close $tf
       if { $nodata ==1 } {
        incr n_file
        continue
    }
    if  {[catch {   mysqlexec $mysql_handler "load data local infile '$insertfile'into table download Fields Terminated By ',' ( ipaddr,  addr, addr_2, addr_3, agency,date, max, avi, purpose, status,YM,day)" } error ]}  {
      puts $error
   }  
       

    incr n_file
  
}

mysqlclose $mysql_handler


package require mysqltcl
global mysqlstatus

set port {3306}
set host {127.0.0.1}
set purpose "netcenter"
set user {ipv6bgp}
set password {ipv6}
set dbname {data}
set dltbl {dlfiles}
set unzipfile /home/rsync/nctemp
set insertfile /home/mysql/data/dltemp2
#have to be this dir
cd /home/rsync/files/
set flist [lsort [glob $purpose*]]
set mysql_handler [mysqlconnect -host $host -port $port -user $user -password $password]
mysqluse $mysql_handler $dbname
mysqlexec $mysql_handler "set names 'utf8'"
set var '$purpose%'
set lastfile [mysqlsel $mysql_handler "select id, filename from $dltbl where id = (select max(id) from $dltbl where filename like $var)" -list]
set lastfile [lindex $lastfile 0 1]
set n_file [lsearch $flist $lastfile]
incr n_file
#set files [glob sohu*]
while { [set read_file [lindex $flist $n_file]]!=""} {
   puts  $read_file
   set tempfilename $read_file
   if {[string match *zip $read_file]!= 0 } {
            exec unzip -p $read_file > $unzipfile
            set fp [open $unzipfile r]
            set read_file $unzipfile
            set zipped 1
          } else {
          if {[string match *gz $read_file]!= 0 } {
            #set tempfilename $read_file
            exec gzip  -dc $read_file > $unzipfile
            set fp [open $unzipfile r]
            set read_file $unzipfile
            set zipped 1
          } else {

                   set fp [open $read_file r]
                   set zipped 0
          }}
   set tf [open $insertfile w]
   fconfigure $fp -encoding utf-8
#   fconfigure $fp -encoding gb2312
   set nodata 1
   set line 0
   set firstline 1
            # next line edition

   while {[eof $fp] != 1} {
         #  if {$zipped == 1 && $firstline == 1} {
         #   gets $fp msg
         #  set firstline 2
         # }
         #  if {$zipped ==1} {   gets $fp msg }
           gets $fp msg
            #puts "a"
           if { $msg == "" } {
                continue }
           set msg [string trimleft $msg " "]
           if {$firstline ==1 } {
              if { [string index $msg 0] != "\u7f51"} {
                             close $fp
                             incr firstline
                             set fp [open $read_file r]
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
            lappend msg ",$purpose,$status,$partition_col,$day"
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
    set init_time [clock seconds]
    set init_time [clock format $init_time -format  {%Y-%m-%d %H:%M:%S}]
    if  {[catch {   mysqlexec $mysql_handler "load data  infile '$insertfile'into table download Fields Terminated By ',' ( ipaddr,  addr, addr_2, addr_3, agency,date, max, avi, purpose, status,YM,day)" } error ]}  {
      puts $error
   } else { 
       # mysql 5.5 will not work with 'local' option
       set read_file $tempfilename
       mysqlexec $mysql_handler "insert into dlfiles (filename, starttime, endtime,`lines`) values ('$read_file','$init_time', now(),'$line')"
   }

       incr n_file
  
}

mysqlclose $mysql_handler


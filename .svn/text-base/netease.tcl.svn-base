package require mysqltcl
global mysqlstatus

set port {3306}
set host {127.0.0.1}
set user {ipv6bgp}
set password {ipv6}
set dbname {data}
set unzipfile /home/rsync/temp
set insertfile /var/lib/mysql/data/temp1
cd /home/rsync/files/
set flist [lsort [glob netease*]]
set mysql_handler [mysqlconnect -host $host -port $port -user $user -password $password]
mysqluse $mysql_handler $dbname
set lastfile [mysqlsel $mysql_handler "select id, filename from webfiles where  id = (select max(id) from webfiles where filename like 'netease%') " -list]
set lastfile [lindex $lastfile 0 1]
puts $lastfile
set n_file [lsearch $flist $lastfile]
incr n_file

# start time
while { [set read_file [lindex $flist $n_file]]!=""} {
   puts $read_file
   exec gzip -dc $read_file > $unzipfile
   set fp [open $unzipfile r]
   set tf [open $insertfile w]
#fconfigure $fp -encoding gb2312
  
   set line 1
   while { [eof $fp] != 1} {
     gets $fp msg
     if {$msg == ""} { continue }
     set num 0
     set count 1
     set chrct [string index $msg $num  ]
     while {$chrct !=""} {
     if {$chrct =="/"} {
          set msg [string replace $msg $num $num ","]
          incr count
          if {$count>2} { break }
          }
     #replace slash with comma
     incr num
     set chrct [string index $msg $num  ]
     }
   set msg [string map {",," ",-,"} $msg] ;# foreign entry may have some error
   set msg [string map { " " "_"} $msg]
   set partition_col [string map {"," " "} $msg]
   set partition_col [lindex $partition_col 6] 
   set hours [string range $partition_col 8 9]
   set partition_col [string rang $partition_col 0 5]
   if { $hours >=19 && $hours < 23 }  {
                     set status "b"
            } elseif {
            $hours >= 1 && $hours <6
            }  {
                     set status "i"
           } else {set status "o"}
   set msg $msg,$partition_col,$status
   puts $tf $msg
   incr line
#   puts $line
  }
   close $fp
   close $tf
   set init_time [clock seconds]
   set init_time [clock format $init_time -format  {%Y-%m-%d %H:%M:%S}]
  mysqlexec $mysql_handler "load data  infile '$insertfile'  ignore into table webbrowsing Fields Terminated By ','
   (name,     ip,province,
    city,county,carrier,date, os,tcp,terminal,browser,url,1stscreen,1stpage,pagesize,domready,prtcol,status)"
   mysqlexec $mysql_handler "insert into webfiles (filename,starttime, endtime) values ('$read_file','$init_time',now())"

   incr n_file
}
mysqlclose $mysql_handler   


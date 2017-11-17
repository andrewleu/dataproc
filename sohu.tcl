package require mysqltcl
global mysqlstatus
set port {3306}
set host {127.0.0.1}
set user {ipv6bgp}
set password {ipv6}
set dbname {data}
set ipinfo {ipinformation}
set unzipfile /home/rsync/shtemp
set insertfile /var/lib/mysql/data/shtemp1
cd /home/rsync/files
if { [catch {set flist [lsort [glob sohu*]]} error] } {
    puts $error
    exit 1
}
set init_time [clock seconds]
set init_time [clock format $init_time -format  {%Y-%m-%d %H:%M:%S}]
# start time
set mysql_handler [mysqlconnect -host $host -port $port -user $user -password $password]
set infobase [ mysqlconnect -host $host -port $port -user $user -password $password]
mysqluse $infobase $ipinfo
mysqluse $mysql_handler $dbname
set lastfile [mysqlsel $mysql_handler "select id, filename from webfiles where id = (select max(id) from webfiles where filename like 'sohu%')" -list]
set lastfile [lindex $lastfile 0 1]
set n_file [lsearch $flist $lastfile]
incr n_file
#set files [glob sohu*]
while { [set read_file [lindex $flist $n_file]]!=""} {
   exec gzip -dc $read_file > $unzipfile
   set fp [open $unzipfile r]
   set tf [open $insertfile w]
   fconfigure $fp -encoding gb2312
   #fconfigure $fp -encoding gb2312
   gets $fp msg
   set line 1
            # next line edition
   while {$msg != ""} {
            while { [string index $msg 0] != "\u641c" && $msg != "" } {
            gets $fp msg
            set msg [string map {"\"" ""} $msg]
            } ; # find the starting line
            if { $msg != ""}  {
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
           gets $fp msg
           set msg [string map {"\"" ""} $msg]
       }
      }
       close $fp
       close $tf
       mysqlexec $mysql_handler "load data  infile '$insertfile'into table webbrowsing Fields Terminated By ',' ( name,     ip,province,
        city,county,carrier,date, os,tcp,terminal,browser,url,1stscreen,1stpage,pagesize,domready,prtcol,status)"
       mysqlexec $mysql_handler "insert into webfiles (filename, starttime, endtime) values ('$read_file','$init_time', now())"
       incr n_file;puts line
}
mysqlclose $mysql_handler
mysqlclose $infobase

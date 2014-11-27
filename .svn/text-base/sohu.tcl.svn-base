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
set flist [lsort [glob sohu*]]

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
            gets $fp msg } ; # find the starting line
            if { $msg != ""}  {  
  if 0 {
 # ......
# /* following line is for the uplevel location searching
                set num 0
                set count 0
                set chrct [string index $msg $num]
                set field_begin 0
                set field_end 0
                while {$chrct !=  ""} {
                     if {$chrct ==","} {
                             incr count
                          if {$count == 2}  {
                               set field_begin $num
                          
                           }   elseif { $count ==3 }    {
                               set field_end $num
                               break
                         } 
                        }   
                        incr num    
                        set chrct [string index $msg $num]
                     }     ; # abstracting the location name
           set location [    string rang $msg [expr $field_begin+1] [expr $field_end-1]]
           set end_word [string index $location end ]
           set location [string replace $location end end "%"] ; # for searching, use wild card
           set locid [mysqlsel $infobase  "select id, pid from region where name like '$location' " -list]
           if { [lindex $locid 0 1] !=0} { ; #the location name is at first level? no 
                set uplevel [mysqlsel $infobase "select id, pid, name from region where id = '[lindex $locid 0 1]'" -list]
                set parent [lindex $uplevel 0 2]
                set msg [string replace $msg $field_begin $field_begin ",$parent,"] ; #insert the parent field
                set field_end [expr $field_end+[  string length $parent]+1] ; # increase the end pointer include the coma byte
                if { [lindex $uplevel 0 1] !=0 } { ; # the location name is at third level? yes
                           set uplevel [mysqlsel $infobase "select id, pid, name from region where id = '[lindex $uplevel 0 1]'" -list ]
                           set parent [lindex $uplevel 0 2]
                           set msg [string replace $msg $field_begin $field_begin ",$parent,"]
                           set field_end [expr $field_end+[  string length $parent]]
                } else { ; # the location name is at second level, filling the third field
                           set msg [string replace $msg $field_end $field_end ",-,"]
                     }
           } else  { ; #location name is at first level
                 set msg [ string replace $msg $field_end $field_end ",-,-,"]
           }  
# end of uplevel searching */
} 
           puts $tf $msg
           gets $fp msg
           incr line
           puts $line
       }   
      }
       close $fp
       close $tf
       mysqlexec $mysql_handler "load data  infile '$insertfile'into table webbrowsing Fields Terminated By ',' ( name,     ip,province,
        city,county,carrier,date, os,tcp,terminal,browser,url,1stscreen,1stpage,pagesize,domready)"
       mysqlexec $mysql_handler "insert into webfiles (filename, starttime, endtime) values ('$read_file','$init_time', now())"
       incr n_file
}     
  
mysqlclose $mysql_handler
mysqlclose $infobase   


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
set insertfile /var/lib/mysql/data/dltemp2
#have to be this dir
cd /home/rsync/files
set flist [lsort [glob $purpose*.gz]]
  set mysql_handler [mysqlconnect -host $host -port $port -user $user -password $password]
 mysqluse $mysql_handler $dbname
set var '$purpose%'
set lastfile [mysqlsel $mysql_handler "select id, filename from $dltbl where id = (select max(id) from $dltbl where filename like $var)" -list]
set lastfile [lindex $lastfile 0 1]
set n_file [lsearch $flist $lastfile]
incr n_file
#set files [glob sohu*]
while { [set read_file [lindex $flist $n_file]]!=""} {
    #set read_file "chinacache_20130501205026.txt"
   puts  $read_file
   if {[string match *zip $read_file]!= 0 } {
   	    exec unzip -c $read_file > $unzipfile
   	    set fp [open $unzipfile r]
   	    set zipped 1
   	  } else {
          if {[string match *gz $read_file]!= 0 } {
            #set tempfilename $read_file
            exec gzip  -dc $read_file > $unzipfile
            set fp [open $unzipfile r]
            set zipped 1
          } 
          
   	  	   set fp [open $read_file r]
   	  	   set zipped 0
   	  }
   set tf [open $insertfile w]
   fconfigure $fp -encoding utf-8
   #fconfigure $fp -encoding gb2312
 
   set line 1
   set firstline 1
            # next line edition
 
   while {[eof $fp] != 1} {
           if {$zipped == 1 && $firstline == 1} {
            gets $fp msg
            gets $fp msg
          }
            gets $fp msg
            if { $zipped == 1} { gets $fp msg }


            #puts "a"
           if { $msg == "" } {
           	continue }
           if { [string index $msg 0] != "\u7f51"} {
                  if {$firstline ==1 } {
                       #       close $fp
                            incr firstline
                       #      set fp [open $read_file r]
                       #      fconfigure $fp -encoding utf-8
                             gets $fp msg
                             if { [string index $msg 0] != "\u7f51"} { 
                                set nodata 1
                                break
                             } 
                           } else {
                           	continue }
              }
           set nodata 0
            if { [string length $msg] <20 } {continue }
            set msg [string map { "\u7f51\u5bbf," ""} $msg]
            lappend msg ",$purpose,w"
            #puts "replace"
      #      set msg [lappend msg $strtail]
            set msg [string map {" " ""} $msg]
            puts $tf $msg
            incr line
            if {[ expr {$line%10000}] ==0} { puts $line }
       }
        
       close $fp
       close $tf
       if { $nodata ==1 } {
        incr n_file
        continue
    }
    set init_time [clock seconds]
    set init_time [clock format $init_time -format  {%Y-%m-%d %H:%M:%S}]
       mysqlexec $mysql_handler "load data  infile '$insertfile'into table download_$purpose Fields Terminated By ',' ( ipaddr,  addr, addr_2, addr_3, agency,date, max, avi, purpose, status)"
       # mysql 5.5 will not work with 'local' option
       mysqlexec $mysql_handler "insert into dlfiles (filename, starttime, endtime) values ('$read_file','$init_time', now())"
       incr n_file
   
}

mysqlclose $mysql_handler

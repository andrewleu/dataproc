package require mysqltcl
global mysqlstatus
set port {3306}
#set host {172.24.20.68}
set host {127.0.0.1}
set purpose "chinacache"
set user {ipv6bgp}
set password {ipv6}
set dbname {data}
set dltbl "dlfiles"
set unzipfile "./ungz.txt"
#set unzipfile /home/rsync/dltemp
set insertfile /home/dltemp1
#have to be this dir
cd /home/rsync/files
set flist [lsort [glob $purpose*]]
#start time
set mysql_handler [mysqlconnect -host $host -port $port -user $user -password $password]
mysqluse $mysql_handler $dbname
mysqlexec $mysql_handler "set names 'utf8'"
set var '$purpose%'
set lastfile [mysqlsel $mysql_handler "select id, filename,entry_num from $dltbl where id = (select max(id) from $dltbl where filename like $var)" -list]
set prv_entry [lindex $lastfile 0 2]
set lastfile [lindex $lastfile 0 1]
set n_file [lsearch $flist $lastfile]
incr n_file
set zipped 0 
#set files [glob sohu*]
while { [set read_file [lindex $flist $n_file]] != "" } {
   #set read_file "chinacache_20130501205026.txt" # to handle gziped file
 set morefiles 0
 set out ""
 if {[string match *gz $read_file]!= 0 } {  #only a gzip file
     if {[catch  {exec gzip -dc $read_file > $unzipfile} out]==0} { 
            set fp [open $unzipfile r]
            set orig_name $read_file
            set read_file $unzipfile
            set zipped 1 } #one gzip file
 } else {  #a gzip file without gz postfix 
     set postfix [lindex [split $read_file "."] 1] ;# splitted parts have same postfix
     set postfix [string range $postfix 0 9]
     exec cat $read_file > cc.gz
     incr n_file
     while {[set tempfilename [lindex $flist $n_file]]!=""} { 
       # the file is splitted into parts
       if {[string first  $postfix $tempfilename  ]!=-1 } { ; #the postfix in the filename 
              exec cat $tempfilename >> cc.gz  ; #restore the gz file
              incr n_file                      
              puts "$tempfilename , more";    
              set lastpart $tempfilename
              set morefiles 1; #more files follow the first one with 'aa'
       } else  {
             break
       }
     }
     if { [catch {exec zcat cc.gz  > $unzipfile } out]==0 } {
            set  fp [open $unzipfile r] ; #unzipped file 
            set orig_name $read_file
            set read_file $unzipfile
            set zipped 1; puts $unzipfile
       } else {
            puts "zip error"  
            puts $out;# plain txt
            #set fp [open "cc.gz" r]
            #set zipped 0
            #incr n_file 
            continue
       }
   }
   #puts $read_file
   #set fp [open $read_file r]
   set tf [open $insertfile w]
   fconfigure $fp -encoding utf-8; #fconfigure $tf -encoding utf-8
   #fconfigure $fp -encoding gb2312
   set line  0
   while {[eof $fp] != 1} {
         gets $fp msg
         if {$line==0} {
          puts $msg
         }
         if { [string length $msg] > 30} { 
            set num 0
            set count 0
            set msg [string map {"\t" ","} $msg]
            set chrct [string index $msg $num]
            while { $chrct!=""&& $count<6} {
              if { $chrct ==","} {
                  incr count
              }
              incr num  
              set chrct [string index $msg $num]
              }
            set strtail [ string range $msg $num end]
            set msg [string range $msg 0 $num-1]
            set strtail [string map {"," " "} $strtail]
            set part1 [lindex $strtail  0]
            set part2 [lindex $strtail  1]
            if {$part1 < $part2 } {
              set swap $part1
              set part1 $part2
              set part2 $swap
            }
             set msg [string map { " " "_"} $msg]
            set partition_col [string map {"," " "} $msg]
            set partition_col [lindex $partition_col 5] 
            set hours [string rang $partition_col 8 9]
            set day [string rang $partition_col 6 7]
            set partition_col [string rang $partition_col 0 5]
           if { $hours >=19 && $hours < 23 }  {
                     set status "b"
            } elseif {
            $hours >= 1 && $hours <6
            }  {
                     set status "i"
           } else {set status "o"}
              set msg "$msg$part1,$part2,$purpose,$status,$partition_col,$day,$orig_name"
      #      set msg [lappend msg $strtail]
           
            puts $tf $msg
            incr line
            if {[expr {$line % 500000}] == 0} {puts $line; puts $msg}
          }
   } 
  close $fp
  close $tf
  set init_time [clock seconds]
  set init_time [clock format $init_time -format  {%Y-%m-%d %H:%M:%S}]
  if  { [ catch {mysqlexec $mysql_handler "load data local infile '$insertfile'into table download Fields Terminated By ',' ( ipaddr,  addr, addr_2, addr_3, agency,date, max, avi, purpose, status,YM,day)" } error] } {
    puts "open $read_file with $error"
  } else {
 # mysql 5.5 will not work with 'local' option
  set entry_num [mysqlsel $mysql_handler "select max(id) from download" -list]
  # where purpose='chinacache' and ym=$partition_col and day=$day and id>$prv_entry" -list]
  if {$zipped==1} {
   set read_file $orig_name
  } ;# store the original reading file name and replace the unzipped file name.
  mysqlexec $mysql_handler "insert into dlfiles (filename, starttime, endtime,`lines`, entry_num) values ('$read_file','$init_time', now(),'$line','$entry_num')"
}
incr n_file
  if {$morefiles !=0 } {
      mysqlexec $mysql_handler "insert into dlfiles (filename, starttime, endtime,`lines`,entry_num) values ('$lastpart','$init_time', now(),'$line','$entry_num')" ; set morefiles 0} 
}
 
mysqlclose $mysql_handler


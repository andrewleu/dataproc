#!/bin/bash
export LANG=en_US.UTF-8
cd /home/dataproc/
tclsh ./dl_netcenterV1.tcl #> ./mailsrc
tclsh ./dl_qihuV1.tcl  #>>./mailsrc
tclsh ./dl_chinacacheV1.tcl #>>./mailsrc
#tclsh ./cc.gzip.tcl  #>>mailsrc
tclsh ./dl_youku.tcl #>>mailsrc
#tclsh ./dl_ku6.tcl  #>>mailsrc
python ./dl_tencent.py # >> mailsrc
tclsh ./web/netease_v2.tcl #>>mailsrc
tclsh ./web/sohu.tcl #>>mailsrc
tclsh ./web/tencent.tcl #>>mailsrc
#cat ./mailsrc | mail -s insert andrew

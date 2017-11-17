#!/bin/bash
export LANG=en_US.UTF-8
cd /home/dataproc/
tclsh ./dl_netcenterV1.tcl
tclsh ./dl_qihuV1.tcl
tclsh ./dl_chinacacheV1.tcl
tclsh ./dl_youku.tcl
tclsh ./dl_ku6.tcl
python ./dl_tencent.py
tclsh ./web/netease_v2.tcl
tclsh ./web/sohu.tcl
tclsh ./web/tencent.tcl

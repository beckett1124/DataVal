#!/bin/bash

for i in `cat /data1/qibin/workspace/mofang/log/test.log|grep False |awk '{print $7}'`; do
    t=`ls -lh /data1/qibin/workspace/mofang/data |grep -v bak|grep $i`
    echo $t | awk 'BEGIN{OFS="\t"}{if($14 > 0){print $9,$5,$18,$14 >> "normal.txt"} else {print $9,$5,$18,$14 >> "un_normal.txt"}}'
done


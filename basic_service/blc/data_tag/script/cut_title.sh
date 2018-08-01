#!/bin/sh

input=$1
output=$2
hadoop fs -rm -r -f $output

hadoop jar $HADOOP_STREAMING \
  -D mapred.job.name="bgn_cut_title@zhangyunfei" \
  -D mapred.reduce.tasks=32 \
  -files ./script/cut_title.py  \
  -input $input \
  -output $output \
  -mapper "anaconda2/anaconda2/bin/python2.7 cut_title.py" \
  -reducer "cat" \
  -cacheArchive /user/jd_ad/zhangyunfei13/tool/anaconda2.tar.gz#anaconda2

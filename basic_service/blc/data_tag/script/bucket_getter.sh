#!/bin/sh

input=$1
output=$2
hadoop fs -rm -r -f $output

hadoop jar $HADOOP_STREAMING \
  -D mapred.job.name="bgn_bucket_getter@zhangyunfei" \
  -D mapred.reduce.tasks=1 \
  -files ./script/bucket_getter.py \
  -input $input \
  -output $output \
  -mapper "python bucket_getter.py" \
  -reducer "cat"

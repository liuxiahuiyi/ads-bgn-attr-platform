#!/bin/sh

input=$1
output=$2
vocab_file=$3
topk=$4
multiple=$5

hadoop fs -rm -r -f $output

hadoop jar $HADOOP_STREAMING \
  -D mapred.job.name="bgn_select_data@zhangyunfei" \
  -D mapred.reduce.tasks=1 \
  -files ./script/select_mapper.py,./script/select_reducer.py,./data/$vocab_file \
  -input $input \
  -output $output \
  -mapper "python select_mapper.py $vocab_file $topk $multiple" \
  -reducer "python select_reducer.py $multiple"

#!/bin/sh

input_path="hdfs://ns3/user/jd_ad/ads_anti/taotong/bgn/segmentation"
output_path="hdfs://ns3/user/jd_ad/ads_anti/taotong/bgn/word_count"

hadoop fs -rm -r -f $output_path


hadoop jar "/software/servers/hadoop-2.2.0/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar" \
  -D mapred.job.name="bgn_word_count" \
  -D mapred.reduce.tasks=200 \
  -files mapper.py,reducer.py \
  -input $input_path \
  -output $output_path \
  -mapper "python mapper.py" \
  -reducer "python reducer.py"

hadoop fs -cat $output_path/* > ./word_count.txt

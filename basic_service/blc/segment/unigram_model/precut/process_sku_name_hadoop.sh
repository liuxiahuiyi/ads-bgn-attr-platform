#!/bin/sh
date=$1
category=$2

sku_data_path="hdfs://ns3/user/jd_ad/ads_bgn/ad_bgn.db/sku_attribute/dt=$date/category=$category"
output_path="hdfs://ns3/user/jd_ad/ads_anti/taotong/bgn/segmentation"
user_dict="user_dict.txt"

raw_num=`hadoop fs -count $sku_data_path | awk '{print $2}'`
reducer_num=$[$raw_num-1]

if [ $reducer_num -lt 1 -o $reducer_num -gt 10000 ]; then
  echo "reducer number: "$reducer_num
  echo "Illegal reducer number, should be in [1,10000]. Please check ad_bgn_sku_attribute in hive"
  exit
fi

echo "reducer number: "$reducer_num

hadoop fs -rm -r -f $output_path

hadoop jar "/software/servers/hadoop-2.2.0/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar" \
  -D mapred.job.name="bgn_process_sku_title" \
  -D mapred.reduce.tasks=$reducer_num \
  -files process_sku_name_hadoop.py,$user_dict \
  -input $sku_data_path \
  -output $output_path \
  -mapper "anaconda2/anaconda2/bin/python2.7 process_sku_name_hadoop.py map $user_dict" \
  -reducer "cat" \
  -cacheArchive /user/jd_ad/zhangyunfei13/tool/anaconda2.tar.gz#anaconda2


#!/bin/sh
date=$1
category=$2

raw_num=`hadoop fs -count hdfs://ns3/user/jd_ad/ads_bgn/ad_bgn.db/sku_attribute/dt=$date/category=$category | awk '{print $2}'`
reducer_num=$[$raw_num-1]

if [ $reducer_num -lt 1 -o $reducer_num -gt 10000 ]; then
  echo "reducer number: "$reducer_num
  echo "Illegal reducer number, should be in [1,10000]. Please check ad_bgn_sku_attribute in hive"
  exit 1
fi

echo "reducer number: "$reducer_num

cd ../cut_title
pwd
sh process_sku_name_hadoop.sh $date $category $reducer_num

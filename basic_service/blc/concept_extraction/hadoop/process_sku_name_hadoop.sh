#!/bin/sh
date=$1
category=$2

sku_data_path="hdfs://ns3/user/jd_ad/ads_bgn/ad_bgn.db/sku_attribute/dt=$date/category=$category"
output_path="hdfs://ns3/user/jd_ad/ads_bgn/ad_bgn.db/taxonomy_sku_segmentation/dt=$date/category=$category"
local_dir="."
user_dict_dir="user_dict"
user_dict_file="user_dict.txt"

hive -e "insert overwrite local directory '$local_dir/$user_dict_dir'
         ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
         select word
         from ad_bgn.taxonomy_internal_user_dict
         where dt = '$date'
         and category = '$category' 
        "
cat $local_dir/$user_dict_dir/* > $local_dir/$user_dict_file

hadoop fs -rm -r -f $output_path

hadoop jar "/software/servers/hadoop-2.2.0/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar" \
  -D mapred.job.name="bgn_process_sku_title" \
  -D mapred.reduce.tasks=100 \
  -files process_sku_name_hadoop.py,$local_dir/$user_dict_file \
  -input $sku_data_path \
  -output $output_path \
  -mapper "anaconda2/anaconda2/bin/python2.7 process_sku_name_hadoop.py map $user_dict_file" \
  -reducer "cat" \
  -cacheArchive /user/jd_ad/zhangyunfei13/tool/anaconda2.tar.gz#anaconda2

hive -e "msck repair table ad_bgn.taxonomy_sku_segmentation"


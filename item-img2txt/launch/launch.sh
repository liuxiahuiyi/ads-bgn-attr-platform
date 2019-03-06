#!/bin/bash
dir=$(dirname "$0")
today=`date +"%Y-%m-%d" -d "$1 -1 days"`
target_db_table=$2
target_db_table_split=(${target_db_table//./ })
target_db=${target_db_table_split[0]}
target_table=${target_db_table_split[1]}
library_hdfs=$3
ocr_trained_data_hdfs=$4
item_first_cate_cds=$5
hdfs_prefix='hdfs://ns1018/user/jd_ad/ads_aof/'
clear_date=`date +"%Y-%m-%d" -d "$1 -3 days"`
jars=$(ls -al $dir/../lib|grep "^-"|awk -v root="${dir}" 'BEGIN{ORS=","}{print root"/../lib/"$9}')

hive -f "${dir}/create_table.sql" \
  -hivevar current_date=${today} \
  -hivevar clear_date=${clear_date} \
  -hivevar target_db=${target_db} \
  -hivevar target_table=${target_table} \
  -hivevar hdfs_prefix=${hdfs_prefix}


spark-submit \
  --name ads_bgn_item_img2txt \
  --class com.jd.bgn.item_img2txt.Runner \
  --master yarn \
  --deploy-mode client \
  --executor-memory 15g \
  --executor-cores 3 \
  --num-executors 400 \
  --queue root.bdp_jmart_ad_data.jd_ad_data_low \
  --driver-memory 5g \
  --jars ${jars} \
  --conf spark.storage.memoryFraction=0.3 \
  --conf spark.locality.wait.node=0 \
  --conf spark.yarn.submit.waitAppCompletion=true \
  --conf spark.default.parallelism=15000 \
  --conf spark.sql.shuffle.partitions=15000 \
  --conf spark.executorEnv.LC_ALL=C \
  --conf spark.yarn.appMasterEnv.LC_ALL=C \
  --conf spark.network.timeout=1200 \
  $dir/../lib/item-img2txt-1.0-SNAPSHOT.jar \
  --date ${today} \
  --target_db_table ${target_db_table} \
  --library_hdfs ${library_hdfs} \
  --ocr_trained_data_hdfs ${ocr_trained_data_hdfs} \
  --item_first_cate_cds ${item_first_cate_cds}



#!/bin/bash
set -e
dir=$(dirname "$0")
today=`date +"%Y-%m-%d" -d "$1 -1 days"`
taget_db_table=$2
library_hdfs=$3
ocr_trained_data_hdfs=$4
item_first_cate_cds=$5
clear_date=`date +"%Y-%m-%d" -d "$1 -3 days"`
jars=$(ls -al $dir/../lib|grep "^-"|awk -v root="${dir}" 'BEGIN{ORS=","}{print root"/../lib/"$9}')

hive -f "${dir}/create_table.sql" \
  -hivevar current_date=${today} \
  -hivevar clear_date=${clear_date} \
  -hivevar taget_db_table=${taget_db_table}

spark-submit \
  --name ads_bgn_item_img2txt \
  --class com.jd.bgn.item_img2txt.Runner \
  --master yarn \
  --deploy-mode client \
  --executor-memory 10g \
  --executor-cores 3 \
  --num-executors 400 \
  --queue root.bdp_jmart_sz_union.bdp_jmart_sz_data_low \
  --driver-memory 5g \
  --jars ${jars} \
  --conf spark.storage.memoryFraction=0.4 \
  --conf spark.locality.wait.node=0 \
  --conf spark.yarn.submit.waitAppCompletion=true \
  --conf spark.default.parallelism=10000 \
  --conf spark.sql.shuffle.partitions=10000 \
  --conf spark.executorEnv.LC_ALL=C \
  --conf spark.yarn.appMasterEnv.LC_ALL=C \
  --conf spark.network.timeout=1200 \
  $dir/../lib/item-img2txt-1.0-SNAPSHOT.jar \
  --date ${today} \
  --taget_db_table ${taget_db_table} \
  --library_hdfs ${library_hdfs} \
  --ocr_trained_data_hdfs ${ocr_trained_data_hdfs} \
  --item_first_cate_cds ${item_first_cate_cds}

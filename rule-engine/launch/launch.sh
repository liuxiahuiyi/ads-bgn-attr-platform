#!/bin/bash
set -e
dir=$(dirname "$0")
today=`date +"%Y-%m-%d" -d "$1 -1 days"`
target_db_table=$2
target_db_table_split=(${target_db_table//./ })
target_db=${target_db_table_split[0]}
target_table=${target_db_table_split[1]}
rule_file=$3
if [ -z $4 ]; then
  repartition=50
else
  repartition=$4
fi
if [ -z $5 ]; then
  parallelism=2000
else
  parallelism=$5
fi
hdfs_prefix='hdfs://ns1018/user/jd_ad/ads_aof/'
clear_date=`date +"%Y-%m-%d" -d "$1 -3 days"`
hive -f "${dir}/create_table.sql" \
  -hivevar today=${today} \
  -hivevar clear_date=${clear_date} \
  -hivevar target_db=${target_db} \
  -hivevar target_table=${target_table} \
  -hivevar hdfs_prefix=${hdfs_prefix}

spark-submit \
  --name ads_bgn_attr_platform_rule_engine \
  --class com.jd.bgn.rule_engine.Runner \
  --master yarn \
  --deploy-mode client \
  --executor-memory 20g \
  --executor-cores 4 \
  --num-executors 200 \
  --queue root.bdp_jmart_ad_data.jd_ad_data_low \
  --driver-memory 6g \
  --conf spark.storage.memoryFraction=0.4 \
  --conf spark.locality.wait.node=0 \
  --conf spark.yarn.submit.waitAppCompletion=true \
  --conf spark.default.parallelism=${parallelism} \
  --conf spark.sql.shuffle.partitions=${parallelism} \
  $dir/../lib/rule-engine-1.0-SNAPSHOT.jar \
  --date ${today} \
  --target_db_table ${target_db_table} \
  --source_id 36 \
  --repartition ${repartition} \
  --use_local_attr_set false \
  --rule_file ${rule_file}

partitions=`hadoop fs -ls "${hdfs_prefix}/${target_db}.db/${target_table}/"|awk '{print $8}'`
for p in ${partitions[@]}; do
  if [[ ${p: -10} < "${clear_date}" ]]; then
    hadoop fs -rm -r "${p}"
  fi
done
hive -e "ALTER TABLE ${target_db_table} DROP IF EXISTS PARTITION (dt<'${clear_date}')"
hive -e "MSCK REPAIR TABLE ${target_db_table}"


#!/bin/bash
set -e
dir=$(dirname "$0")
today=`date +"%Y-%m-%d" -d "$1 -1 days"`
taget_db_table=$2
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
clear_date=`date +"%Y-%m-%d" -d "$1 -5 days"`
hive -f "${dir}/create_table.sql" \
  -hivevar today=${today} \
  -hivevar clear_date=${clear_date} \
  -hivevar taget_db_table=${taget_db_table}

spark-submit \
  --name ads_bgn_attr_platform_rule_engine \
  --class com.jd.bgn.rule_engine.Runner \
  --master yarn \
  --deploy-mode client \
  --executor-memory 20g \
  --executor-cores 4 \
  --num-executors 200 \
  --queue root.bdp_jmart_sz_union.bdp_jmart_sz_data_high \
  --driver-memory 6g \
  --conf spark.storage.memoryFraction=0.4 \
  --conf spark.locality.wait.node=0 \
  --conf spark.yarn.submit.waitAppCompletion=true \
  --conf spark.default.parallelism=${parallelism} \
  --conf spark.sql.shuffle.partitions=${parallelism} \
  $dir/../lib/rule-engine-1.0-SNAPSHOT.jar \
  --date ${today} \
  --taget_db_table ${taget_db_table} \
  --source_id 36 \
  --repartition ${repartition} \
  --use_local_attr_set false \
  --rule_file ${rule_file}

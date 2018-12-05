#!/bin/bash
set -e
dir=$(dirname "$0")
target_db_table="ad_bgn.bgn_attr_platform_rule_engine_test"
target_db='ad_bgn'
target_table='bgn_attr_platform_rule_engine_test'
today="2018-10-30"
clear_date="2018-10-01"
rule_file="${dir}/../conf/rules_test.properties"
hdfs_prefix='hdfs://ns1018/user/jd_ad/ads_aof/'

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
  --conf spark.default.parallelism=2000 \
  --conf spark.sql.shuffle.partitions=2000 \
  $dir/../lib/rule-engine-1.0-SNAPSHOT.jar \
  --date ${today} \
  --target_db_table ${target_db_table} \
  --source_id 36 \
  --repartition 50 \
  --use_local_attr_set false \
  --rule_file ${rule_file}


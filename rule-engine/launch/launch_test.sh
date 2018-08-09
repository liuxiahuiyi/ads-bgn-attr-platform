#!/bin/bash
set -e
dir=$(dirname "$0")
taget_db_table="bgn_test.bgn_attr_platform_rule_engine"
today="2018-08-30"
clear_date="2018-07-25"
rule_file="${dir}/../conf/rules_test.properties"
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
  --queue root.bdp_jmart_sz_union.bdp_jmart_sz_data_low \
  --driver-memory 6g \
  --conf spark.storage.memoryFraction=0.4 \
  --conf spark.locality.wait.node=0 \
  --conf spark.yarn.submit.waitAppCompletion=true \
  --conf spark.default.parallelism=2000 \
  --conf spark.sql.shuffle.partitions=2000 \
  $dir/../lib/rule-engine-1.0-SNAPSHOT.jar \
  --date ${today} \
  --taget_db_table ${taget_db_table} \
  --source_id 36 \
  --repartition 50 \
  --use_local_attr_set false \
  --rule_file ${rule_file}


#!/bin/sh

date=$1
attri_vocab=$2

input="/user/jd_ad/ads_bgn/ad_bgn.db/sku_attribute/dt=$date/*"
output="/user/jd_ad/zhangyunfei13/bgn/attri_vocab/"

hadoop fs -rm -r -f $output

hadoop jar $HADOOP_STREAMING \
  -D mapred.job.name="bgn_segment_attri_vocab@zhangyunfei" \
  -D mapred.reduce.tasks=1 \
  -files ./script/attri_mr.py \
  -input $input \
  -output $output \
  -mapper "python attri_mr.py Map" \
  -reducer "python attri_mr.py Reduce"

hadoop fs -text /user/jd_ad/zhangyunfei13/bgn/attri_vocab/* > $attri_vocab

#!/bin/sh
category=$1

tar -zcvf word_prob.tar.gz word_prob.pkl

hadoop fs -rm -r -f hdfs://ns3/user/jd_ad/ads_anti/taotong/bgn/word_prob/$category
hadoop fs -mkdir hdfs://ns3/user/jd_ad/ads_anti/taotong/bgn/word_prob/$category
hadoop fs -put word_prob.tar.gz hdfs://ns3/user/jd_ad/ads_anti/taotong/bgn/word_prob/$category/
hadoop fs -put ../precut/user_dict.txt hdfs://ns3/user/jd_ad/ads_anti/taotong/bgn/word_prob/$category/
hadoop fs -put ../precut/product_words.txt hdfs://ns3/user/jd_ad/ads_anti/taotong/bgn/word_prob/$category/


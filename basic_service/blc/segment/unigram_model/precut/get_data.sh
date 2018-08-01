#!/bin/sh
category=$1

mkdir -p data

hive -e "insert overwrite local directory './data'
         select distinct product_words_list 
         from ad_bgn.ad_bgn_sku_attribute
         where category='$category'"

cat ./data/* > product_words_raw.txt

hive -e "insert overwrite local directory './data'
         select distinct brandname_cn 
         from ad_bgn.ad_bgn_sku_attribute
         where category='$category'"

cat ./data/* > brandname_raw.txt


#!/bin/sh

date=$1
note_file=$2
pw_vocab=$3
brand_vocab=$4

output_path='./temp'
mkdir -p $output_path

product_model="./vocab/product_moode.dat"
date='2018-03-26'
hive -e "INSERT OVERWRITE LOCAL DIRECTORY '$output_path'
         row format delimited
         fields terminated by '\t' 
         select 
             item_type, count(item_type) 
         from ad_bgn.ad_bgn_sku_attribute
         where dt='$date'
         and item_type!=''
         group by item_type
         "
cat $output_path/* > $product_model
exit 0

# export label data
hive -e "INSERT OVERWRITE LOCAL DIRECTORY '$output_path'
         row format delimited
         fields terminated by '\t' 
         select 
              user_account, 
              skuid, 
              skuname, 
              label
         from ad_bgn.label_segmentation
         where dt='$date'
         "
cat $output_path/* > $note_file

# export product vocab
date='2018-02-25'
hive -e "INSERT OVERWRITE LOCAL DIRECTORY '$output_path'
         row format delimited
         fields terminated by '\t' 
         select 
              product_word
         from ad_bgn.taxonomy_internal_product_words
         where dt='$date'
         "
cat $output_path/* > $pw_vocab

# export brand vocab
date='2018-02-25'
hive -e "INSERT OVERWRITE LOCAL DIRECTORY '$output_path'
         row format delimited
         fields terminated by '\t' 
         select 
              brand 
         from ad_bgn.taxonomy_internal_sku_brands
         where dt='$date'
         "
cat $output_path/* > $brand_vocab


#!/bin/sh

#outpath="/user/jd_ad/zhangyunfei13/bgn/sku_name/clothe/"
#hive -e "insert overwrite directory '$outpath'
#         ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
#         select item_sku_id, sku_name
#         from gdm.gdm_m03_item_sku_da
#         where dt = '2018-01-10'
#         and item_first_cate_cd = '1315'
#         and sku_valid_flag = 1
#         and sku_status_cd != '3000'
#         and sku_status_cd != '3010'
#         and item_sku_id is not null
#         "

input="/user/jd_ad/zhangyunfei13/bgn/sku_name/clothe/"
output="/user/jd_ad/zhangyunfei13/bgn/cut/clothe/"
bash ./script/cut_title.sh $input $output

input="/user/jd_ad/zhangyunfei13/bgn/cut/clothe/"
output="/user/jd_ad/zhangyunfei13/bgn/vocab/clothe/"
bash ./script/bucket_getter.sh $input $output
hadoop fs -cat $output/* | awk -F '\t' '{a[$1]+=$2}END{for(w in a){print w,a[w]}}' >  ./data/vocab.dat

input="/user/jd_ad/zhangyunfei13/bgn/cut/clothe/"
output="/user/jd_ad/zhangyunfei13/bgn/data_tag/clothe"
topk=100000
multiple=2
bash ./script/select_data.sh $input $output vocab.dat $topk $multiple

sku_id_path="/user/jd_ad/zhangyunfei13/bgn/data_tag/clothe/"
sku_name_path="/user/jd_ad/zhangyunfei13/bgn/sku_name/clothe/"
output_path="/user/jd_ad/zhangyunfei13/bgn/data_tag/output/clothe/"
hadoop fs -rm -f -r $output_path
pig -useHCatalog \
  -p output_path=${output_path} \
  -p sku_id_path=${sku_id_path} \
  -p sku_name_path=${sku_name_path} \
  pig/merge_title.pig
mv pig_*.log log

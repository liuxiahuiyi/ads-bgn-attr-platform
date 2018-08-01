#!/bin/sh
date=$1
category=$2
reduce_num=$3

function export_vocab(){
  output_path="./temp/${date}_${category}"
  mkdir -p $output_path
  # export product vocab
  hive -e "INSERT OVERWRITE LOCAL DIRECTORY '$output_path'
           row format delimited
           fields terminated by '\t' 
           select 
                product_word
           from ad_bgn.taxonomy_internal_product_words_v2
           where dt='$date'
           "
  pw_vocab="product_word.dat"
  cat $output_path/* > $pw_vocab

  # export brand vocab
  hive -e "INSERT OVERWRITE LOCAL DIRECTORY '$output_path'
           row format delimited
           fields terminated by '\t' 
           select 
                brand 
           from ad_bgn.taxonomy_internal_sku_brands_v2
           where dt='$date'
           "
  brand_vocab="brand_word.dat"
  cat $output_path/* > $brand_vocab
}

function predict(){
  input="hdfs://ns3/user/jd_ad/ads_bgn/ad_bgn.db/taxonomy_sku_basic_segmentation/dt=$date/category=$category"
  output="hdfs://ns3/user/jd_ad/ads_bgn/ad_bgn.db/taxonomy_sku_pos_tag/dt=$date/category=$category"

  hadoop fs -rm -r -f $output
  
  hadoop jar $HADOOP_STREAMING \
    -D mapred.job.name="bgn_pos_tag@zhangyunfei" \
    -D mapreduce.map.memory.mb=25000 \
    -D mapred.reduce.tasks=$reduce_num \
    -files predict.py,model_factory.py,load_data.py,model.py,utils.py,config.yml,util.py,feature_extractor.py,brand_word.dat,product_word.dat,config.py \
    -input $input \
    -output $output \
    -mapper "anaconda2/anaconda2/bin/python2.7 predict.py Map" \
    -reducer "cat" \
    -cacheArchive /user/jd_ad/zhangyunfei13/tool/anaconda2.tar.gz#anaconda2 \
    -cacheArchive /user/jd_ad/zhangyunfei13/bgn/blc/pos_tag/model/Res.tar.gz#Res \
    -cacheArchive /user/jd_ad/zhangyunfei13/bgn/blc/pos_tag/model/Model.tar.gz#Model
  
  if [ $? != 0 ]
    exit 1

  hive -e "msck repair table ad_bgn.taxonomy_sku_pos_tag"
}

function pack_model(){
  mkdir -p temp
  cd ../lstm_crf_tool/Model
  /bin/rm -rf Model.tar.gz
  tar -zcvf Model.tar.gz *
  mv Model.tar.gz ../../predict/temp/

  cd ../Res
  /bin/rm -rf Res.tar.gz
  tar -zcvf Res.tar.gz *
  mv Res.tar.gz ../../predict/temp/
  cd ../../predict

  hadoop fs -rm -r -f /user/jd_ad/zhangyunfei13/bgn/blc/pos_tag/model/Res.tar.gz
  hadoop fs -rm -r -f /user/jd_ad/zhangyunfei13/bgn/blc/pos_tag/model/Model.tar.gz

  hadoop fs -put temp/Model.tar.gz /user/jd_ad/zhangyunfei13/bgn/blc/pos_tag/model/
  hadoop fs -put temp/Res.tar.gz /user/jd_ad/zhangyunfei13/bgn/blc/pos_tag/model/
}

#pack_model

export_vocab

predict

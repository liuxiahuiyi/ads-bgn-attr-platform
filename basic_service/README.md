1.获取word2vec 和 train的数据，在bi_tao堡垒机执行
  sh get_w2v_data.sh ITEM_SECOND_CATE_CD date(取数日期) NUMBER
  (将get_data_all_orig一起上传)sh get_train_data.sh com_attr_cd  ITEM_SECOND_CATE_CD 

2.将 1 中得到的orig_w2v.txt 和 orig_tr.txt 分别放到./data 和 ./data/"ITEM_SECOND_CATE_CD"_"com_attr_cd"中，上传到gpu机器。
3.更新run.py 中 ITEM_SECOND_CATE_CD和com_cate_cd，执行run.py
4.将生成的ITEM_SECOND_CATE_CD"_"com_attr_cd"_streaming 文件夹打包发送到bi_tao堡垒机，执行sh run.sh ITEM_SECOND_CATE_CD com_attr_cd 生成结果到bgn.clothes_attr_online PARTITION (dt,item_second_cate_cd,com_attr_cd)
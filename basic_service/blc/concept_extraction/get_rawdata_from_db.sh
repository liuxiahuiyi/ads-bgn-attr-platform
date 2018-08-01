if [ ! -d "./rawdata" ]; then
  mkdir ./rawdata
fi

hive -e "
    select item_sku_id 
    from ad_bgn.ad_bgn_sku_attribute
    where item_first_cate_cd in (670, 737, 9987, 652)
    and dt = '2018-02-04'
    ;
" > rawdata/cellphone_item_sku_id.txt

hive -e "
    select sku_name
    from ad_bgn.ad_bgn_sku_attribute
    where item_first_cate_cd in (670, 737, 9987, 652)
    and dt = '2018-02-04'
    ;
" > rawdata/cellphone_sku_name.txt

paste -d "\t" rawdata/cellphone_item_sku_id.txt rawdata/cellphone_sku_name.txt > rawdata/cellphone_sku_id_name.txt

hive -e "
    select distinct(brandname_en) 
    from ad_bgn.ad_bgn_sku_attribute
    where item_first_cate_cd in (670, 737, 9987, 652)
    and dt = '2018-02-04'
    ;
" > rawdata/cellphone_brandname_en.txt

hive -e "
    select distinct(brandname_cn) 
    from ad_bgn.ad_bgn_sku_attribute
    where item_first_cate_cd in (670, 737, 9987, 652)
    and dt = '2018-02-04'
    ;
" > rawdata/cellphone_brandname_cn.txt

hive -e "
    select distinct(brandname_full) 
    from ad_bgn.ad_bgn_sku_attribute
    where item_first_cate_cd in (670, 737, 9987, 652)
    and dt = '2018-02-04'
    ;
" > rawdata/cellphone_brandname_full.txt

hive -e "
select distinct(product_word) from
ad_bgn.ad_bgn_sku_attribute 
lateral view explode(split(product_words_list,', ')) ss as product_word
where item_first_cate_cd in (670, 737, 9987, 652) and product_words_list is not null
and dt = '2018-02-04'
" > rawdata/product_words.txt

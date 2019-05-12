CREATE TABLE IF NOT EXISTS ${hivevar:db}.${hivevar:table} (
  item_second_cate_cd     STRING COMMENT 'item second cate id',
  item_third_cate_cd      STRING COMMENT 'item third cate id',
  item_sku_id             STRING COMMENT 'item sku id',
  sku_name                STRING COMMENT 'sku name',
  com_attr_name           STRING COMMENT 'attr name',
  old_attr_value_cd       STRING COMMENT 'old attr value id',
  old_attr_value_name     STRING COMMENT 'old attr value name',
  com_attr_value_cd       STRING COMMENT 'attr value id',
  com_attr_value_name     STRING COMMENT 'attr value name',
  source                  STRING COMMENT 'algorithm id'
)
PARTITIONED BY (
  dt                      STRING COMMENT 'take date',
  item_first_cate_cd      STRING COMMENT 'item first cate id',
  com_attr_cd             STRING COMMENT 'attr id'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '${hivevar:hdfs_prefix}/${hivevar:db}.db/${hivevar:table}';

ALTER TABLE ${hivevar:db}.${hivevar:table} DROP IF EXISTS PARTITION (dt<'${hivevar:clear_date}') PURGE;
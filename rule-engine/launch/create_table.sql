CREATE TABLE IF NOT EXISTS ${hivevar:target_db}.${hivevar:target_table} (
  item_second_cate_cd STRING COMMENT 'item second category id',
  item_third_cate_cd  STRING COMMENT 'item third category id',
  item_sku_id         STRING COMMENT 'item sku id',
  sku_name            STRING COMMENT 'sku title',
  barndname_full      STRING COMMENT 'brand',
  colour              STRING COMMENT 'colour',
  size                STRING COMMENT 'size',
  item_type           STRING COMMENT 'item type',
  com_attr_cd         STRING COMMENT 'common attribute name id',
  com_attr_name       STRING COMMENT 'common attribute name',
  com_attr_value_cd   STRING COMMENT 'common attribute value id',
  com_attr_value_name STRING COMMENT 'common attribute value',
  com_attr_value_rem  STRING COMMENT 'common attribute value remark',
  com_attr_group      STRING COMMENT 'common attribute group',
  alt_attr_value_cd   STRING COMMENT 'alternative attribute value id',
  alt_attr_value_name STRING COMMENT 'alternative attribute value',
  old_attr_value_cd   STRING COMMENT 'old attribute value id',
  old_attr_value_name STRING COMMENT 'old attribute value',
  source              INT    COMMENT 'source algorithm from'
)
PARTITIONED BY (
  dt                  STRING COMMENT 'take date',
  flag                STRING COMMENT 'whether recovery or supplement',
  item_first_cate_cd  STRING COMMENT 'item first category id'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '${hivevar:hdfs_prefix}/${hivevar:target_db}.db/${hivevar:target_table}'
TBLPROPERTIES ('orc.compress'='SNAPPY');

ALTER TABLE ${hivevar:target_db}.${hivevar:target_table} DROP IF EXISTS PARTITION (dt<'${hivevar:clear_date}') PURGE;

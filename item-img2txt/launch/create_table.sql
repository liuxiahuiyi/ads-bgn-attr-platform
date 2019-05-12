CREATE TABLE IF NOT EXISTS ${hivevar:target_db}.${hivevar:target_table} (
  item_id             STRING COMMENT 'item id',
  item_img_url        STRING COMMENT 'item detail image url',
  item_img_txt        STRING COMMENT 'item detail image text',
  start_date          STRING COMMENT 'start date'
)
PARTITIONED BY (
  dp                  STRING COMMENT 'take date',
  end_date            STRING COMMENT 'end date',
  item_first_cate_cd  STRING COMMENT 'item first category id'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '${hivevar:hdfs_prefix}/${hivevar:target_db}.db/${hivevar:target_table}'
TBLPROPERTIES ('orc.compress'='SNAPPY');

ALTER TABLE ${hivevar:target_db}.${hivevar:target_table} DROP IF EXISTS PARTITION (dp='EXPIRE', end_date<'${hivevar:clear_date}') PURGE;


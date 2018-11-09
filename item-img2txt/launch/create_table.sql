CREATE TABLE IF NOT EXISTS ${hivevar:taget_db_table} (
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
TBLPROPERTIES ('orc.compress'='SNAPPY');

ALTER TABLE ${hivevar:taget_db_table} DROP IF EXISTS PARTITION (dp='EXPIRE', end_date<'${hivevar:clear_date}');
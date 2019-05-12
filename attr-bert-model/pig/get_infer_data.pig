skus = LOAD 'gdm.gdm_m03_item_sku_act' USING org.apache.hive.hcatalog.pig.HCatLoader();
skus = FILTER skus BY (
  (dt == '${current_date}') AND
  (item_first_cate_cd == '${item_first_cate_cd}') AND
  (sku_valid_flag == 1) AND
  (sku_status_cd != '3000') AND
  (sku_status_cd != '3010') AND
  (item_sku_id IS NOT NULL) AND
  (item_sku_id != '') AND
  (sku_name IS NOT NULL) AND
  (sku_name != '')
);
skus = FOREACH skus GENERATE item_first_cate_cd, item_second_cate_cd, item_third_cate_cd, item_sku_id, sku_name;
skus = DISTINCT skus;

ext_attrs = LOAD 'gdm.gdm_m03_item_sku_ext_attr_da' USING org.apache.hive.hcatalog.pig.HCatLoader();
ext_attrs = FOREACH ext_attrs GENERATE cate_id,item_sku_id,com_attr_cd,com_attr_name,com_attr_value_cd,com_attr_value_name,dt;
spec_attrs = LOAD 'gdm.gdm_m03_item_sku_spec_par_da' USING org.apache.hive.hcatalog.pig.HCatLoader();
spec_attrs = FOREACH spec_attrs GENERATE cate_id,item_sku_id,com_attr_cd,com_attr_name,com_attr_value_cd,com_attr_value_name,dt;
raw_attrs = UNION ext_attrs, spec_attrs;
raw_attrs = FILTER raw_attrs BY (
  (dt == '${current_date}') AND
  (com_attr_cd == '${com_attr_cd}') AND
  (item_sku_id IS NOT NULL) AND
  (item_sku_id != '')
);
item_third_cate_cds = FOREACH skus GENERATE item_third_cate_cd;
item_third_cate_cds = DISTINCT item_third_cate_cds;
filtered_attrs = JOIN raw_attrs BY cate_id, item_third_cate_cds BY item_third_cate_cd USING 'replicated';
filtered_attrs = FOREACH filtered_attrs GENERATE
  raw_attrs::item_sku_id         AS item_sku_id,
  raw_attrs::com_attr_cd         AS com_attr_cd,
  raw_attrs::com_attr_name       AS com_attr_name,
  raw_attrs::com_attr_value_cd   AS com_attr_value_cd,
  raw_attrs::com_attr_value_name AS com_attr_value_name;
attrs = FOREACH (GROUP filtered_attrs BY (item_sku_id, com_attr_cd, com_attr_name)) {
  tmp = ORDER filtered_attrs BY com_attr_value_cd DESC;
  com_attr_value = LIMIT tmp 1;
  GENERATE
    FLATTEN(group)                          AS (item_sku_id, com_attr_cd, com_attr_name),
    MAX(com_attr_value.com_attr_value_cd)   AS com_attr_value_cd,
    MAX(com_attr_value.com_attr_value_name) AS com_attr_value_name;
}


joined = JOIN skus BY item_sku_id LEFT, attrs BY item_sku_id;
joined = FOREACH joined GENERATE
  skus::item_first_cate_cd   AS item_first_cate_cd,
  skus::item_second_cate_cd  AS item_second_cate_cd,
  skus::item_third_cate_cd   AS item_third_cate_cd,
  skus::item_sku_id          AS item_sku_id,
  skus::sku_name             AS sku_name,
  attrs::com_attr_cd         AS com_attr_cd,
  attrs::com_attr_name       AS com_attr_name,
  attrs::com_attr_value_cd   AS com_attr_value_cd,
  attrs::com_attr_value_name AS com_attr_value_name;

third_cate_ratio = FOREACH (GROUP joined BY (item_first_cate_cd, item_second_cate_cd, item_third_cate_cd)) {
  has_attr = FILTER joined BY (com_attr_cd IS NOT NULL);
  has_attr_count = COUNT(has_attr);
  total_count = COUNT(joined);
  ratio = (double)has_attr_count / total_count;
  GENERATE
    FLATTEN(group)             AS (item_first_cate_cd, item_second_cate_cd, item_third_cate_cd),
    ratio                      AS ratio;
}
data = JOIN joined BY (item_first_cate_cd, item_second_cate_cd, item_third_cate_cd), third_cate_ratio BY (item_first_cate_cd, item_second_cate_cd, item_third_cate_cd) USING 'replicated';
data = FOREACH data GENERATE
  joined::item_first_cate_cd   AS item_first_cate_cd,
  joined::item_second_cate_cd  AS item_second_cate_cd,
  joined::item_third_cate_cd   AS item_third_cate_cd,
  joined::item_sku_id          AS item_sku_id,
  joined::sku_name             AS sku_name,
  '${com_attr_cd}'             AS com_attr_cd,
  joined::com_attr_name        AS com_attr_name,
  joined::com_attr_value_cd    AS com_attr_value_cd,
  joined::com_attr_value_name  AS com_attr_value_name,
  third_cate_ratio::ratio      AS ratio;
data = FILTER data BY (
  (com_attr_name IS NOT NULL) OR
  (ratio > 0.9)
);

attr_name = FOREACH attrs GENERATE com_attr_cd, com_attr_name;
attr_name = LIMIT attr_name 1;
result = JOIN data BY com_attr_cd, attr_name BY com_attr_cd USING 'replicated';
result = FOREACH result GENERATE
  data::item_first_cate_cd     AS item_first_cate_cd,
  data::item_second_cate_cd    AS item_second_cate_cd,
  data::item_third_cate_cd     AS item_third_cate_cd,
  data::item_sku_id            AS item_sku_id,
  data::sku_name               AS sku_name,
  data::com_attr_cd            AS com_attr_cd,
  attr_name::com_attr_name     AS com_attr_name,
  data::com_attr_value_cd      AS com_attr_value_cd,
  data::com_attr_value_name    AS com_attr_value_name;

STORE result INTO '${infer_data_hdfs}' USING PigStorage('\t');



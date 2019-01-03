REGISTER '${formatter}' USING jython AS formatter;
skus = LOAD 'gdm.gdm_m03_item_sku_act' USING org.apache.hive.hcatalog.pig.HCatLoader();
skus = FILTER skus BY (
  (dt == '${current_date}') AND
  (item_first_cate_cd == '${item_first_cate_cd}') AND
  (sku_valid_flag == 1) AND
  (sku_status_cd != '3000') AND
  (sku_status_cd != '3010') AND
  (item_id IS NOT NULL) AND
  (item_id != '') AND
  (item_sku_id IS NOT NULL) AND
  (item_sku_id != '') AND
  (sku_name IS NOT NULL) AND
  (sku_name != '') AND
  (item_id != item_sku_id)
);
skus = FOREACH skus GENERATE item_first_cate_cd, item_second_cate_cd, item_third_cate_cd, item_id, item_sku_id, sku_name;
skus = DISTINCT skus;

orders = LOAD 'gdm.gdm_m04_ord_det_sum' USING org.apache.hive.hcatalog.pig.HCatLoader();
orders = FILTER orders BY (
  (
    (dt <= '${current_date}') OR
    (dt == '4712-12-31')
  ) AND
  (dt >= '${current_date_60}') AND
  (item_first_cate_cd == '${item_first_cate_cd}') AND
  (sale_ord_valid_flag == '1') AND
  (free_goods_flag != '1') AND
  (item_sku_id IS NOT NULL) AND
  (item_sku_id != '') AND
  (sale_ord_dt IS NOT NULL) AND
  (sale_ord_dt != '') AND
  (sale_ord_dt <= '${current_date}') AND
  (sale_ord_dt >= '${current_date_60}')
);
orders = FOREACH (GROUP orders BY item_sku_id) GENERATE
  group                   AS item_sku_id,
  SUM(orders.sale_qtty)   AS sale_qtty;

ext_attrs = LOAD 'gdm.gdm_m03_item_sku_ext_attr_da' USING org.apache.hive.hcatalog.pig.HCatLoader();
ext_attrs = FOREACH ext_attrs GENERATE cate_id,item_sku_id,com_attr_cd,com_attr_name,com_attr_value_cd,com_attr_value_name,dt;
spec_attrs = LOAD 'gdm.gdm_m03_item_sku_spec_par_da' USING org.apache.hive.hcatalog.pig.HCatLoader();
spec_attrs = FOREACH spec_attrs GENERATE cate_id,item_sku_id,com_attr_cd,com_attr_name,com_attr_value_cd,com_attr_value_name,dt;
raw_attrs = UNION ext_attrs, spec_attrs;
raw_attrs = FILTER raw_attrs BY (
  (dt == '${current_date}') AND
  (com_attr_cd == '${com_attr_cd}') AND
  (item_sku_id IS NOT NULL) AND
  (item_sku_id != '') AND
  (com_attr_value_cd IS NOT NULL) AND
  (com_attr_value_cd != '') AND
  (com_attr_value_name IS NOT NULL) AND
  (com_attr_value_name != '')
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
filtered_attrs = DISTINCT filtered_attrs;

attrs_count = FOREACH (GROUP filtered_attrs BY com_attr_cd) GENERATE
  group                  AS com_attr_cd,
  COUNT(filtered_attrs)  AS count;
attrs_ratio = FOREACH (GROUP filtered_attrs BY (com_attr_cd, com_attr_value_cd, com_attr_value_name)) GENERATE
  FLATTEN(group)         AS (com_attr_cd, com_attr_value_cd, com_attr_value_name),
  COUNT(filtered_attrs)  AS attr_count;
attrs_ratio = JOIN attrs_ratio BY com_attr_cd, attrs_count BY com_attr_cd USING 'replicated';
attrs_ratio = FOREACH attrs_ratio GENERATE
  attrs_ratio::com_attr_value_cd    AS com_attr_value_cd,
  attrs_ratio::com_attr_value_name  AS com_attr_value_name,
  (double)attrs_ratio::attr_count / (attrs_count::count + 0.001) AS attr_ratio;
attrs = JOIN filtered_attrs BY (com_attr_value_cd, com_attr_value_name), attrs_ratio BY (com_attr_value_cd, com_attr_value_name) USING 'replicated';
attrs = FOREACH attrs GENERATE
  filtered_attrs::item_sku_id         AS item_sku_id,
  filtered_attrs::com_attr_cd         AS com_attr_cd,
  filtered_attrs::com_attr_name       AS com_attr_name,
  filtered_attrs::com_attr_value_cd   AS com_attr_value_cd,
  filtered_attrs::com_attr_value_name AS com_attr_value_name,
  attrs_ratio::attr_ratio             AS attr_ratio;


ordered_skus = JOIN skus BY item_sku_id, orders BY item_sku_id;
ordered_skus = FOREACH ordered_skus GENERATE
  skus::item_first_cate_cd     AS item_first_cate_cd,
  skus::item_second_cate_cd    AS item_second_cate_cd,
  skus::item_third_cate_cd     AS item_third_cate_cd,
  skus::item_id                AS item_id,
  skus::item_sku_id            AS item_sku_id,
  skus::sku_name               AS sku_name,
  orders::sale_qtty            AS sale_qtty;
ordered_skus = FILTER ordered_skus by (
  (sale_qtty > 5)
);

train_data = JOIN ordered_skus BY item_sku_id, attrs BY item_sku_id;
train_data = FOREACH train_data GENERATE
  ordered_skus::item_first_cate_cd  AS item_first_cate_cd,
  ordered_skus::item_second_cate_cd AS item_second_cate_cd,
  ordered_skus::item_third_cate_cd  AS item_third_cate_cd,
  ordered_skus::item_id             AS item_id,
  ordered_skus::item_sku_id         AS item_sku_id,
  ordered_skus::sku_name            AS sku_name,
  attrs::com_attr_cd                AS com_attr_cd,
  attrs::com_attr_name              AS com_attr_name,
  attrs::com_attr_value_cd          AS com_attr_value_cd,
  attrs::com_attr_value_name        AS com_attr_value_name,
  attrs::attr_ratio                 AS attr_ratio;

formatted_train_data = FOREACH (GROUP train_data BY (item_first_cate_cd, item_second_cate_cd, item_third_cate_cd, item_id, com_attr_cd, com_attr_name)) {
  format = formatter.format(group, train_data.(item_sku_id, sku_name, com_attr_value_cd, com_attr_value_name, attr_ratio));
  GENERATE
    group.item_first_cate_cd   AS item_first_cate_cd,
    group.item_second_cate_cd  AS item_second_cate_cd,
    group.item_third_cate_cd   AS item_third_cate_cd,
    group.item_id              AS item_id,
    group.com_attr_cd          AS com_attr_cd,
    group.com_attr_name        AS com_attr_name,
    FLATTEN(format)            AS (item_sku_id: chararray, sku_name: chararray, com_attr_value_cd: chararray, com_attr_value_name: chararray);
}
formatted_train_data = LIMIT formatted_train_data ${limits};

STORE formatted_train_data INTO '${train_data_hdfs}' USING PigStorage('\t');




  

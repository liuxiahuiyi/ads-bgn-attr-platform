SET mapred.map.tasks.speculative.execution true;
SET mapred.reduce.tasks.speculative.execution true;
SET mapreduce.map.memory.mb 4096;
SET mapreduce.reduce.memory.mb 8192;
SET mapred.compress.map.output true;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET default_parallel 128;

sku_ids = load '${sku_id_path}' as (sku_id:chararray, word:chararray);
sku_names = load '${sku_name_path}' as (sku_id:chararray, name:chararray);

sku_names_join = JOIN sku_ids by sku_id, sku_names by sku_id;
sku_names_join = foreach sku_names_join generate sku_ids::sku_id as sku_id,
                                                 sku_names::name as sku_name;
store sku_names_join into '${output_path}';

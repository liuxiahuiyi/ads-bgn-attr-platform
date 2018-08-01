import codecs
import re
from filter_non_concepts import check_exact_product_word

#input_file_sku_id = 'rawdata/cellphone_item_sku_id.txt'
input_file_sku_name_processed = 'intermediate/cellphone_sku_name_processed.txt'
#input_file_sku_name = 'rawdata/cellphone_sku_name.txt'
input_file_sku_id_name = 'rawdata/cellphone_sku_id_name.txt'
concept_file = 'result/concepts.txt'
output_file = 'result/sku_concept_rel.txt'
product_file = 'rawdata/product_words.txt'

sku_id_list = list()
sku_name_list = list()
sku_name_processed_list = list()
concept_list = set()
product_list = set()

concept_re_dict = dict()
product_re_dict = dict()

with codecs.open(input_file_sku_id_name, 'r', 'utf-8') as f:
  for line in f.readlines():
    ss = line.strip().split('\t')
    if len(ss) < 2:
      continue
    sku_id_list.append(ss[0])
    sku_name_list.append(ss[1])
#with codecs.open(input_file_sku_name, 'r', 'utf-8') as f:
#  for line in f.readlines():
#    sku_name_list.append(line.strip())
with codecs.open(input_file_sku_name_processed, 'r', 'utf-8') as f:
  for line in f.readlines():
    sku_name_processed_list.append(line.strip())
with codecs.open(concept_file, 'r', 'utf-8') as f:
  for line in f.readlines():
    cn = line.strip()
    concept_list.add(cn)
    concept_re_dict[cn] = re.compile(cn)
with codecs.open(product_file, 'r', 'utf-8') as f:
  for line in f.readlines():
    pn = line.strip()
    product_list.add(pn)
    product_re_dict[pn] = re.compile(pn)

print len(sku_id_list)
print len(sku_name_list)
print len(sku_name_processed_list)

with codecs.open(output_file, 'w', 'utf-8') as f:
  for i in xrange(len(sku_id_list)):
    if i % 10000 == 0:
      print i
    sku_id = sku_id_list[i]
    sku_name = sku_name_list[i]
    sku_name_processed = sku_name_processed_list[i]
    concept_name_list = list()
    concept_pos_list = list()
    product_name_list = list()
    product_pos_list = list()
    for c in sku_name_processed.split():
      if c in product_list:
        if c not in product_name_list:
          product_name_list.append(c)
          product_pos_list.append([i.start() for i in product_re_dict[c].finditer(sku_name)])
      elif c in concept_list:
        if c not in concept_name_list:
          concept_name_list.append(c)
          concept_pos_list.append([i.start() for i in concept_re_dict[c].finditer(sku_name)])
      else:
        pass
    if len(concept_name_list) == 0 or len(product_name_list) == 0:
      continue
    result_str_list = [sku_id, sku_name]
    for ii in range(len(concept_name_list)):
      x = concept_name_list[ii] + ':'
      x += ','.join(str(c) for c in concept_pos_list[ii])
      result_str_list.append(x)
    for ii in range(len(product_name_list)):
      x = 'pro_' + product_name_list[ii] + ':'
      x += ','.join(str(c) for c in product_pos_list[ii])
      result_str_list.append(x)
    result_str = '\01'.join(result_str_list)
    f.write(result_str + '\n')

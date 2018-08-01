import codecs

result = set()
with codecs.open('intermediate/brands.txt', 'r', 'utf-8') as f1, \
     codecs.open('intermediate/colors.txt', 'r', 'utf-8') as f2,\
     codecs.open('intermediate/deduplicated_concepts.txt', 'r', 'utf-8') as f3,\
     codecs.open('rawdata/product_words.txt', 'r', 'utf-8') as f4,\
     codecs.open('intermediate/user_dict.txt', 'w', 'utf-8') as f5:
  for line in f1.readlines():
    result.add(line.strip())
  for line in f2.readlines():
    result.add(line.strip())
  for line in f3.readlines():
    result.add(line.strip())
  for line in f4.readlines():
    result.add(line.strip())
  for s in result:
    f5.write(s+'\n')

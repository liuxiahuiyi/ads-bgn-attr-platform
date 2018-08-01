import codecs

product_words = set()

with codecs.open('intermediate/cellphone_product_words_freq.txt', 'r', 'utf-8') as f:
  for line in f.readlines():
    product_words.add(line.split(',')[0])

with codecs.open('result/concepts_decorated.txt', 'r', 'utf-8') as f1, \
     codecs.open('result/concepts.txt', 'w', 'utf-8') as f2:
  for line in f1.readlines():
    if line.split(':')[0] in product_words:
      continue
    f2.write(line)

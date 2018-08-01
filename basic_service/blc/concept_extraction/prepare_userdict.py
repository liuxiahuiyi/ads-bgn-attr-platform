# coding: utf-8
import codecs


words = list()

with codecs.open('intermediate/sorted_concepts.txt', 'r', 'utf-8') as f:
  for line in f.readlines():
    words.append(line.strip())

with codecs.open('intermediate/user_dict_prepared.txt', 'w', 'utf-8') as f:
  for word in words:
    f.write(word + '\n' + word + u'手机' + '\n' + word + u'机' + '\n')

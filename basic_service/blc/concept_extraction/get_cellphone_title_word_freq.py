# coding: utf-8
import jieba
import jieba.posseg as pseg
import collections
import codecs
import re

jieba.load_userdict("merge_dict_utf8.txt")

words_from_natural = set()

phrase_len_limit = 6
left_preserve = u'(（“‘"\'[【'
right_preserve = u')）”’"\']】'
all_preserve = left_preserve + right_preserve

with codecs.open('rawdata/cellphone_sku_name.txt', 'r', 'utf-8') as f:
  i = 0
  for line in f.readlines():
    phrases = line.strip().split()
    for phrase in phrases:
      if len(phrase) > 1 and len(phrase) <= phrase_len_limit:
        has_filtered_word = False
        for c in all_preserve:
          if c in phrase:
            has_filtered_word = True
            break
        if not has_filtered_word:
          words_from_natural.add(phrase)
    i += 1
    if i % 10000 == 0:
      print('processed %d lines' % i)

with codecs.open('intermediate/cellphone_name_from_natural.txt', 'w', 'utf-8') as f:
  for word in words_from_natural:
    f.write('%s\n' % word)

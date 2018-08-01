# coding: utf-8
import codecs
import collections

# remove . and / and +
punct = u''':!),:;?]}¢'"、。〉》」』】〕〗〞︰︱︳﹐､﹒
﹔﹕﹖﹗﹚﹜﹞！），．：；？｜｝︴︶︸︺︼︾﹀﹂﹄﹏､～￠
々‖•·ˇˉ―--′’”([{£¥'"‵〈《「『【〔〖（［｛￡￥〝︵︷︹︻
︽︿﹁﹃﹙﹛﹝（｛“‘-—_…*/+.'''

signrepl = u' ' * len(punct)

table = {ord(s): unicode(d) for s, d in zip(punct, signrepl)}

phrase_len_limit = 6
phrase_list = list()

with codecs.open('rawdata/cellphone_sku_name.txt', 'r', 'utf-8') as f:
  for line in f.readlines():
    s = line.translate(table)
    phrases = s.strip().split()
    for phrase in phrases:
      if len(phrase) <= phrase_len_limit and len(phrase) > 1:
        phrase_list.append(phrase)

freq = collections.Counter(phrase_list)

with codecs.open('intermediate/cellphone_name_from_natural.txt', 'w', 'utf-8') as f:
  for phrase, ff in freq.items():
      f.write(phrase + '\t' + str(ff) + '\n')

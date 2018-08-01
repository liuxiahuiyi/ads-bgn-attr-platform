# coding: utf-8
import codecs
import jieba
jieba.load_userdict('intermediate/user_dict.txt')

punct = set(u''':!),:;?]}¢'"、。〉》」』】〕〗〞︰︱︳﹐､﹒
﹔﹕﹖﹗﹚﹜﹞！），．：；？｜｝︴︶︸︺︼︾﹀﹂﹄﹏､～￠
々‖•·ˇˉ―--′’”([{£¥'"‵〈《「『【〔〖（［｛￡￥〝︵︷︹︻
︽︿﹁﹃﹙﹛﹝（｛“‘-—_…*/+.''')

signrepl = u' ' * len(punct)
table = {ord(s): unicode(d) for s, d in zip(punct, signrepl)}
phrase_len_limit = 6

with codecs.open('rawdata/cellphone_sku_id_name.txt', 'r', 'utf-8') as fr,\
     codecs.open('intermediate/cellphone_sku_name_processed.txt', 'w', 'utf-8') as fw:
  i = 0
  for line in fr.readlines():
    if i % 100000 == 0:
      print i
    i += 1
    ss = line.strip().split('\t')
    if len(ss) < 2:
      continue
    s = ss[1].translate(table) 
    output = ' '.join(jieba.lcut(s))
    fw.write(output + '\n')

    '''
    line_seg = list()
    for phrase in s.strip().split():
      if len(phrase.strip()) > phrase_len_limit:
        seg_result = jieba.lcut(phrase)
        line_seg.extend(seg_result)
      else:
        line_seg.append(phrase.strip())
    fw.write(' '.join(line_seg) + '\n')
    '''

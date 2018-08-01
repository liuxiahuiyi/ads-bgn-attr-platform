#coding=utf8
import sys
import codecs
import json
import random
import jieba

char_map_dict = {u' ' : '__SPAC__'}

punct_set = u''':!),:;?]}¢'"、。〉》」』】〕〗〞︰︱︳﹐､﹒
﹔﹕﹖﹗﹚﹜﹞！），．：；？｜｝︴︶︸︺︼︾﹀﹂﹄﹏､～￠
々‖•·ˇˉ―--′’”([{£¥'"‵〈《「『【〔〖（［｛￡￥〝︵︷︹︻
︽︿﹁﹃﹙﹛﹝（｛“‘-—_…*'''
punct_set = set(punct_set)

def get_char_feature(char):
  feature = list()
  if char in punct_set:
    feature.append('PUNC')
  elif (char>= 'A' and char<= 'Z') or (char>= 'a' and char<= 'z'):
    feature.append('EN')
  elif (char>= '0' and char<= '9'):
    feature.append('NUM')
  else:
    feature.append('CN')
  return '\t'.join(feature)
  
def convert2bio(note_file, bio_file, bio_jieba_file):
  fin = codecs.open(note_file, 'r', 'utf8')
  fout = codecs.open(bio_file, 'w', 'utf8')
  fout_jieba = codecs.open(bio_jieba_file, 'w', 'utf8')

  for line in fin:
    try:
      items = line.strip().split('\t')
      raw_name = list(items[2])
      name = list()
      for char in raw_name:
        if char not in char_map_dict:
          name.append(char)
        else:
          name.append(char_map_dict[char])

      jieba_labels = list()
      jieba_result = jieba.lcut(items[2])
      for s in jieba_result:
        for i, char in enumerate(list(s)):
          display_char = char
          if display_char in char_map_dict:
            display_char = char_map_dict[display_char]
          if i == 0:
            jieba_labels.append('B')
          else:
            jieba_labels.append('I')
      
      if len(jieba_labels) != len(name):
        print 'length not equal!'

      notes = json.loads(items[3])
      labels = [None for x in range(len(name))]
      features = [None for x in range(len(name))]
      for note in notes:
        ranges = note['ranges']
        text = note['text']
        for rng in ranges:
          start = rng['startOffset']
          end = rng['endOffset']
          for x in range(start, end):
            # 处理标签
            s = text
            if x == start:
              s = 'B_' + s
            else:
              s = 'I_' + s
            labels[x] = s
      # 其他位置赋值
      for x in range(len(labels)):
        if labels[x] is None:
          labels[x] = 'O'
      
      # 抽取字符级别特征
      for x in range(len(features)):
        feat = get_char_feature(name[x])
        features[x] = feat

      for nm, feat, lb, jb_lb in zip(name, features, labels, jieba_labels):
        s = '%s\t%s\t%s\n' % (nm, feat, lb)
        fout.write(s)
        s = '%s\t%s\t%s\t%s\n' % (nm, feat, lb, jb_lb)
        fout_jieba.write(s)
      fout.write('\n')
      fout_jieba.write('\n')
    except Exception, e:
      print 'convert2bio failed e=%s' % (e)
  pass

def load_instances(bio_file):
  instances = list()
  truncks = list()
  f = open(bio_file, 'r')
  for line in f:
    trunck = line.strip()
    if trunck == '':
      if len(truncks) != 0:
        instances.append(truncks)
        truncks = list()
    else: 
      truncks.append(trunck)
  if len(truncks) != 0:
    instances.append(truncks)
  return instances

def split_corpus(bio_file, bio_jieba_file, train_file, test_file, jieba_file):
  ratio = 0.2
  instances = load_instances(bio_file)
  instances_jieba = load_instances(bio_jieba_file)
  total_index = range(len(instances))
  train_index = set(random.sample(total_index, int(len(total_index) * (1 - 0.2))))

  # 开始分割数据集
  ftrain = open(train_file, 'w')
  ftest = open(test_file, 'w')
  fjieba = open(jieba_file, 'w')
  for i, inst in enumerate(instances):
    s = '\n'.join(inst) + '\n\n'
    if i in train_index:
      ftrain.write(s)
    else:
      ftest.write(s)
  for i, inst in enumerate(instances_jieba):
    s = '\n'.join(inst) + '\n\n'
    if i not in train_index:
      fjieba.write(s)
  
if __name__ == '__main__':
  note_file = sys.argv[1]
  bio_file = sys.argv[1] + '.bio'
  bio_jieba_file = bio_file + '.jieba'
  train_file = sys.argv[2]
  test_file = sys.argv[3]
  jieba_file = sys.argv[4]

  convert2bio(note_file, bio_file, bio_jieba_file)
  split_corpus(bio_file, bio_jieba_file, train_file, test_file, jieba_file)

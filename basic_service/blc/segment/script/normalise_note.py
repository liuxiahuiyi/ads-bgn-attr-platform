#coding=utf8
#author=zhangyunfei
import sys
import os
import json
import codecs
import math
from collections import defaultdict
sys.path.append('./')
from util import load_vocab
from basic_segmentation.bgnseg import BgnSeg
g_bgnseg = BgnSeg()

# 归一化用户标注
# 几方面归一化 : 
# 1. 标注错误归一化，主要是标注边界出现空白符的情况
# 2. 标注标签熵很小，主要是归一化错误标注的
# 3. 特定标签的转化，主要是不对一些标签进行处理，这些标签可以单独拿来处理

class NoteTag(object):
  
  def __init__(self, line):
    items = line.strip().split('\t')
    self.noter = items[0]
    self.skuid = items[1]
    self.name = items[2]
    self.tags = list()
    notes = json.loads(items[3])
    tags = list()
    for note in notes:
      concept = note['quote']
      label = note['text']
      for pos in note['ranges']:
        start = pos['startOffset']
        end = pos['endOffset']
        try:
          end = int(end)
        except:
          end = 0
        if end <= start:
          end = start + len(concept)
        assert(start >= 0 and start < end and end <= len(self.name))
        tags.append({'label':label, 'start':start, 'end':end, 'trunck':concept})
    tags.sort(key=lambda x:x['start'])
    pos = 0
    for tag in tags:
      if pos < tag['start']:
        self.tags.append({'label':'UNK', 'start':pos, 'end':tag['start'], 'trunck':self.name[pos:tag['start']]})
      pos = tag['end']
    self.tags.extend(tags)
    self.tags.sort(key=lambda x:x['start'])
    
  def to_string(self):
    notes = list()
    self.tags.sort(key=lambda x:x['start'])
    name, start = '', 0 
    for tag in self.tags:
      if len(tag['trunck']) != 0 and tag['label'] != 'UNK':
        ranges = [{'startOffset':start, 
                   'endOffset':start + len(tag['trunck']),
                  }]
        notes.append({'quote':tag['trunck'],
                      'text':tag['label'],
                      'ranges':ranges})
      start += len(tag['trunck'])
      name += tag['trunck']
    string = '%s\t%s\t%s\t%s' % (self.noter, self.skuid, name, json.dumps(notes, ensure_ascii=False))
    return string

def load_data(note_file):
  note_datas = list()
  fin = codecs.open(note_file, 'r', 'utf8')
  for line in fin:
    try:
      data = NoteTag(line)
      note_datas.append(data)
    except Exception, e:
      print 'load data failed %s' % e
  return note_datas

def normalise_space(note_datas):
  for note in note_datas:
    tags = list()
    for tag in note.tags:
      trunck = tag['trunck'].strip()
      tag['trunck'] = trunck if trunck else ' '
      tags.append(tag)
    note.tags = tags

def normalise_label(note_datas):
  trunck_labels = defaultdict(dict)
  for note in note_datas:
    for tag in note.tags:
      trunck = tag['trunck']
      label = tag['label']
      trunck_labels[trunck][label] = trunck_labels[trunck].get(label, 0) + 1
  # 计算每个标签的熵
  threshold = 0.8
  trunck_entropys = dict()
  for trunck in trunck_labels:
    total = 0
    for label in trunck_labels[trunck]:
      if trunck_labels[trunck][label] != 'UNK':
        total += trunck_labels[trunck][label]
    if total < 20:
      continue
    max_c = 0
    max_label = ''
    for label in trunck_labels[trunck]:
      if label != 'UNK' and trunck_labels[trunck][label] > max_c:
        max_c = trunck_labels[trunck][label]
        max_label = label
    p = max_c / float(total)
    trunck_entropys[trunck] = p
    if p > threshold:
      trunck_labels[trunck] = {max_label:max_c}
  # 打印出来看看
  #fout = codecs.open('entropy.dat', 'w', 'utf8')
  #for trunck in trunck_labels:
  #  if trunck not in trunck_entropys:
  #    continue
  #  s = '%s\t%s\t%s\n' % (trunck, json.dumps(trunck_labels[trunck], ensure_ascii=False), trunck_entropys[trunck])
  #  fout.write(s)
  threshold = 0.8
  for note in note_datas:
    tags = list()
    for tag in note.tags:
      trunck = tag['trunck']
      label = tag['label']
      if label not in trunck_labels[trunck]:
        continue
      tags.append(tag)
    note.tags = tags

def map_label(note_datas, product_words, brand_words):
  '''
  两种map：1. label 的map
           2. 产品词和品牌词归一化
  '''
  label_map_dict = {u'数字和英文' : u'',
                    u'型号词' : u'',
                    u'规格属性' : u'',
                    #u'适用属性' : u'',
                    u'品牌' : u'品牌词',
                    u'产品' : u'产品词'}
  def get_label(label, trunck, product_words, brand_words, label_map):
    if label == 'UNK':
      if trunck in brand_words:
        label = u'品牌词'
      if trunck in product_words:
        label = u'产品词'
    else:
      if label in label_map:
        label = label_map[label]
    return label or 'UNK'

  for note in note_datas:
    tags = list()
    for tag in note.tags:
      trunck = tag['trunck']
      label = tag['label']
      if label != 'UNK':
        tag['label'] = get_label(label, trunck, product_words, brand_words, label_map_dict)
        tags.append(tag)
      else:
        start = tag['start']
        truncks = g_bgnseg.cut(trunck)
        for trunck in truncks:
          tag = {'start':start, 
                 'end':start+len(trunck), 
                 'trunck':trunck, 
                 'label':get_label(label, trunck, product_words, brand_words, label_map_dict)
                }
          start += len(trunck)
          tags.append(tag)
    note.tags = tags

def token_length(note_datas):
   for note in note_datas:
    tags = list()
    for tag in note.tags:
      trunck = tag['trunck']
      label = tag['label']
      if label == 'UNK' and len(trunck) > 5 or \
         label != 'UNK' and len(trunck) > 12:
         continue
      tags.append(tag)
    note.tags = tags

def merge_token(note_datas):
  def do_merge(tags, label):
    trunck = ''
    for tag in tags:
      trunck += tag['trunck']
    return {'start':tags[0]['start'],
            'end':tags[-1]['end'],
            'label':label,
            'trunck':trunck
            }
  for note in note_datas:
    note.tags.sort(key=lambda x:x['start'])
    tags = list()
    prev_label = ''
    prev_tags = list()
    for tag in note.tags:
      label = tag['label']
      if label == 'UNK':
        prev_label = 'UNK'
        prev_tags.append(tag)
        continue
      if prev_label and prev_tags:
        tags.append(do_merge(prev_tags, prev_label))
        prev_tags = list()
        prev_label = ''
      tags.append(tag)
    if prev_label and prev_tags:
      tags.append(do_merge(prev_tags, prev_label))
    note.tags = tags
 
def normalise(note_file, product_word_file, brand_word_file, outfile):
  # 加载资源文件
  product_words = load_vocab(product_word_file)
  brand_words = load_vocab(brand_word_file)
  note_datas = load_data(note_file)

  # 空格归一化
  normalise_space(note_datas)

  # 标签归一化
  normalise_label(note_datas)

  # 标签映射
  map_label(note_datas, product_words, brand_words)

  # 合并token
  merge_token(note_datas)

  # 长度过滤
  token_length(note_datas)

  # 重新写入文件
  fout = codecs.open(outfile, 'w', 'utf8')
  for note in note_datas:
    s = note.to_string() + '\n'
    fout.write(s)

if __name__ == '__main__':
  note_file = sys.argv[1]
  product_word_file = sys.argv[2]
  brand_word_file = sys.argv[3]
  outfile = sys.argv[4]
  normalise(note_file, product_word_file, brand_word_file, outfile)

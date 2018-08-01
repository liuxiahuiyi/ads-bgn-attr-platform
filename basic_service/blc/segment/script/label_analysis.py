#coding=utf8
#author=zhangyufei
import sys
import codecs
import json
from collections import defaultdict

def get_note_concept_label(name, notes):
  '''
  return : list of cut pos tag
  '''
  # 过滤人工标签
  note_list = list()
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
      assert(start >= 0 and start < end and end <= len(name))
      note_list.append((name[start:end], label))
  return note_list

# 分析各个标签的词
def analysis(note_file, out_file):
  fin = codecs.open(note_file, 'r', 'utf8')
  concept_info = defaultdict(dict)

  for line in fin:
    try:
      items = line.strip().split('\t')
      raw_name = items[2]      
      notes = json.loads(items[3])
      note_list = get_note_concept_label(raw_name, notes)
      for concept, label in note_list:
        concept_info[label][concept] = concept_info[label].get(concept, 0) + 1 
    except Exception, e:
      print 'exception=%s' % e
  # print
  fout = codecs.open(out_file, 'w', 'utf8')
  for label in concept_info:
    print >> fout, 'label=%s items=%s' % (label, len(concept_info[label]))
    items = concept_info[label].items()
    items.sort(key=lambda x:x[1], reverse=True)
    total = sum(concept_info[label].values())
    part = 0.
    for no, (concept, count) in enumerate(items):
      part += count
      print >> fout, 'concept=%s count=%s ratio=%s no=%s' % (concept, count, part/total, no)

if __name__ == '__main__':
  note_file = sys.argv[1]
  out_file = sys.argv[2]
  analysis(note_file, out_file)

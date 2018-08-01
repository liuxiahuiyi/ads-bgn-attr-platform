#coding=utf8
import sys
import os
import json
import codecs
from collections import defaultdict
from full_cutter import FullCutter, max_coverage_path_dyn

def create_cutter(train_file):
  vocab_dict = defaultdict(dict)
  f = codecs.open(train_file, 'r', 'utf8')
  for line in f:
    try:
      items = line.strip().split('\t')
      raw_name = items[2]      
      notes = json.loads(items[3])
      concept_labels = dict()
      for note in notes:
        concept = note['quote']
        label = note['text']
        vocab_dict[concept][label] = vocab_dict[concept].get(label, 0) + 1
    except Exception, e:
      print 'create_cutter failed %s' % e
  
  for concept in vocab_dict:
    items = vocab_dict[concept].items()
    items.sort(key=lambda x:x[1], reverse=True)
    vocab_dict[concept] = items[0][0]

  cutter = FullCutter(set(vocab_dict.keys()))
  return cutter, vocab_dict

def label_title(cutter, vocab_dict, title, labels):
  graph = cutter.full_cut(title)
  path = max_coverage_path_dyn(graph)
  for start, end, match_vocab in path:
    label = vocab_dict.get(title[start:end], None)
    if label is None:
      continue
    for i in range(start, end):
      if i == start:
        labels[i] = 'B_' + label
      else:
        labels[i] = 'I_' + label

char_map_dict = {u' ' : '__SPAC__'}
def naive_label(cutter, vocab_dict, test_file, pred_file):
  fin = codecs.open(test_file, 'r', 'utf8')
  fout = codecs.open(pred_file, 'w', 'utf8')

  for idx, line in enumerate(fin):
    try:
      if idx % 1000 == 0:
        print 'process line %s' % idx
      items = line.strip().split('\t')
      raw_name = items[2]
      ground_labels = ['O_UNK' for x in range(len(raw_name))]
      pred_labels = ['O_UNK' for x in range(len(raw_name))]

      # ground labels
      notes = json.loads(items[3])
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
            end = min(start + len(concept), len(raw_name))
          assert(start >= 0 and start < end and end <= len(raw_name))
          for x in range(start, end):
            if x == start:
              ground_labels[x] = 'B_' + label
            else:
              ground_labels[x] = 'I_' + label
      
      # pred labels
      label_title(cutter, vocab_dict, raw_name, pred_labels)

      # write label
      raw_name = [char_map_dict.get(w, w) for w in list(raw_name)]
      for w, g, p in zip(raw_name, ground_labels, pred_labels):
        s = '%s\t%s\t%s\n' % (w, g, p)
        fout.write(s)
      fout.write('\n')
    except Exception, e:
      print 'naive_label failed %s' % e

if __name__ == '__main__':
  train_file = sys.argv[1]
  test_file = sys.argv[2]
  pred_file = sys.argv[3]

  print 'create_cutter '
  cutter, vocab_dict = create_cutter(train_file)
  
  print 'naive_label'
  naive_label(cutter, vocab_dict, test_file, pred_file)

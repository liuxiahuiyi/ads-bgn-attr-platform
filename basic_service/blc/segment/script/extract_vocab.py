#coding=utf8
#author=zhangyunfei
import sys
import os
import codecs
import json
from collections import defaultdict
sys.path.append('./')
from basic_segmentation.bgnseg import BgnSeg

# 切词器
g_bgnseg = BgnSeg()

# 挖掘词表, 基本思路是
# 1. 词表提供基本信息
# 2. 标注提供消歧和未知pattern预测
# 目前只挖掘人工标注中的pattern
# 后续可以从query中挖掘trunck和打标的思路走

def get_note_label(note_file, label_info, out_dir, cut_concept=True):
  care_labels = set([x[0] for x in label_info])
  concept_label_info = defaultdict(dict)
  fin = codecs.open(note_file, 'r', 'utf8')
  for line in fin:
    try:
      items = line.strip().split('\t')
      notes = json.loads(items[3])
      for note in notes:
        label = note['text'].strip()
        concept = note['quote'].strip()
        if cut_concept:
          concepts = g_bgnseg.cut(concept)
        else:
          concepts = [concept]
        if (not concept) or (label not in care_labels):
          continue
        for pos in note['ranges']:
          for concept in concepts:
            concept_label_info[concept][label] = concept_label_info[concept].get(label, 0) + 1
    except Exception, e:
      print e
  min_sum = 10
  max_ratio = 0.8
  label_concept_info = defaultdict(dict)
  for concept in concept_label_info:
    total = sum(concept_label_info[concept].values())
    if total <= min_sum:
      continue
    label_infos = concept_label_info[concept].items()
    label_infos.sort(key=lambda x:x[1], reverse=True)
    part = 0.
    for label, count in label_infos:
      label_concept_info[label][concept] = label_concept_info[label].get(concept, 0) + count
      part += count
      if part / total >= max_ratio:
        break
  # 写入文件
  file_info = {l:p for l, p in label_info}
  for label in label_concept_info:
    concept_info = label_concept_info[label].items()
    concept_info.sort(key=lambda x:x[1], reverse=True)
    fname = os.path.join(out_dir, file_info[label] +'.dat')
    fout = codecs.open(fname, 'w', 'utf8')
    for concept, count in concept_info:
      s = '%s\t%s\n' %(concept, count)
      fout.write(s)

if __name__ == '__main__':
  note_file = sys.argv[1]
  out_dir = './vocab/'
  label_info = [(u'材质属性', 'material'),
                (u'样式属性', 'style'),
                (u'场景属性', 'scene'),
                (u'功能属性', 'function'),
               ]
  get_note_label(note_file, label_info, out_dir, cut_concept=False)

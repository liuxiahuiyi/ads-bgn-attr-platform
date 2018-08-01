#coding=utf8
import sys
import codecs
import json
import random
import jieba
import config
import util as util
import feature_extractor as fex
from vocab_matcher import VocabMatcher
sys.path.append('./')
from basic_segmentation.bgnseg import BgnSeg

# 切词器
g_bgnseg = BgnSeg()

char_map_dict = {u' ' : '__SPAC__'}
char_map_dict_reverse = {v:k for k, v in char_map_dict.items()}

# 加载词表匹配器
def load_matchers(vocab_infos):
  '''
  vocab_files : [(namespace, vocab_file, vocab_type, sep, user_key, value_min_count), ...}
  return : vocab_dim, vocab_matchers, vocab_name_dim_map
  '''
  # get matchers
  vocab_matchers = list()
  all_keys = list()
  for namespace, vocab_file, vocab_type, sep, user_key, value_min_count in vocab_infos:
    matcher = VocabMatcher(vocab_file, vocab_type, sep, user_key, value_min_count)
    keys = matcher.get_keys()
    all_keys.extend(['%s_%s' % (namespace, key) for key in keys])
    vocab_matchers.append((namespace, matcher))
  vocab_dim = len(all_keys) 
  
  # get voacb_name_dim_map
  vocab_name_dim_map = dict()
  for x in range(vocab_dim):
    key = all_keys[x]
    vocab_name_dim_map[key] = x
  return vocab_dim, vocab_matchers, vocab_name_dim_map

# 特征：1. 字符级特征
#       2. 词表匹配特征
def get_feature_matrix(name, vocab_dim, vocab_matchers, vocab_name_dim_map):
  char_feature_matrix = list()
  for x in range(len(name)):
    char_features = fex.get_char_features(name, x)    # 字符级特征
    char_feature_matrix.append(map(str,char_features))

  vocab_feature_matrix = list()
  for i in range(len(name)):
    vocab_feature_matrix.append([0 for x in range(vocab_dim)])
  for x in range(len(name)):
    for namespace, matcher in vocab_matchers:
      fex.get_vocab_features(name, x, matcher, 
                            namespace, vocab_name_dim_map, 
                            vocab_feature_matrix)
  for i in range(len(vocab_feature_matrix)):
    vocab_feature_matrix[i] = map(str, vocab_feature_matrix[i])

  return (char_feature_matrix, vocab_feature_matrix)

def convert2bio_char(note_file, bio_file):
  unknow_label = 'O_UNK'
  vocab_dim, vocab_matchers, vocab_name_dim_map = load_matchers(config.vocab_matcher_infos)

  fin = codecs.open(note_file, 'r', 'utf8')
  fout = codecs.open(bio_file, 'w', 'utf8')
  for line in fin:
    try:
      items = line.strip().split('\t')
      name = items[2]
      notes = json.loads(items[3])
      # 标签
      labels = [unknow_label for x in range(len(name))]
      for note in notes:
        if note['text'] not in config.g_care_labels:
          continue
        for rng in note['ranges']:
          start = rng['startOffset']
          end = rng['endOffset']
          for x in range(start, end):
            if x == start:
              s = 'B_' + note['text']
            else:
              s = 'I_' + note['text']
            labels[x] = s

      # 特征
      feature_matrixs = get_feature_matrix(name, vocab_dim, vocab_matchers, vocab_name_dim_map)
      
      # 输出
      for i in range(len(name)):
        features = list()
        for matrix in feature_matrixs:
          features.extend(matrix[i])
        if features:
          s = "%s\t%s\t%s\n" % (char_map_dict.get(name[i], name[i]), '\t'.join(features), labels[i])
        else:
          s = "%s\t%s\n" % (char_map_dict.get(name[i], name[i]), labels[i])
        fout.write(s)
      fout.write('\n')
    except Exception, e:
      print 'convert2bio failed e=%s' % (e)

class ConceptLabel(object):
  
  def __init__(self, start, end, word, label):
    self.start = start
    self.end = end
    self.word = word
    self.label = label

# 将一个词中的空白符做转换
def convert_space(seq):
  word = ''
  for s in seq:
    word += char_map_dict.get(s, s)
  return word

# 几种控制方式
g_flag_input_cut = True
g_flag_unk_join = False
g_flag_word_cut = False # 基于词切分还是字切分
g_flag_use_meta_labels = True

# 将标注文件转换成bio格式的数据
# 做两个事情：1. 分词和边界转换；2. 特征抽取
def convert2bio_word(note_file, bio_file, product_word_file, brand_word_file):
  # 加载资源文件
  product_words = util.load_vocab(product_word_file)
  brand_words = util.load_vocab(brand_word_file)
  
  unknow_label = 'UNK'
  fin = codecs.open(note_file, 'r', 'utf8')
  fout = codecs.open(bio_file, 'w', 'utf8')
  if g_flag_use_meta_labels:
    meta_labels = set([u'产品词', u'型号词', u'品牌词'])
  else:
    meta_labels = set()

  for line in fin:
    try:
      items = line.strip().split('\t')
      raw_name = items[2]      
      notes = json.loads(items[3])
      
      # 处理标注部分
      concept_labels = dict()
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
          #if not (start >= 0 and start < end and end <= len(raw_name)):
          #  print 'start=%s end=%s raw_name=%s line=%s' % (start, end, len(raw_name), line)
          assert(start >= 0 and start < end and end <= len(raw_name))
          concept_labels[start] = ConceptLabel(start, end, raw_name[start:end], label)

      # 处理未标注部分
      start, end = 0, 0
      while end < len(raw_name):
        if end not in concept_labels:
          end += 1
        else:
          if start < end:
            concept_labels[start] = ConceptLabel(start, end, raw_name[start:end], unknow_label)
          start = concept_labels[end].end
          end = start
      # 处理最后一个concept
      if start < end:
        concept_labels[start] = ConceptLabel(start, end, raw_name[start:end], unknow_label)

      # 作为list处理
      concept_labels = concept_labels.items()
      concept_labels.sort(key=lambda x:x[0])

      # 开始处理各个concept
      for start, concept in concept_labels:
        if g_flag_input_cut:
          # 不切分
          if (g_flag_unk_join and concept.label == unknow_label) or (concept.label in meta_labels):
            features = fex.get_word_features(concept.word, product_words, brand_words)
            label = 'O_' + concept.label if concept.label == unknow_label else 'B_' + concept.label
            s = '%s\t%s\t%s\n' % (convert_space(concept.word), '\t'.join(features), label)
            fout.write(s)
          else: 
            # 切词 && 获取相应特征
            if g_flag_word_cut:
              words = [word for word in jieba.cut(concept.word)]
            else:
              words = list(concept.word)
            features_list = [fex.get_word_features(word, product_words, brand_words) for word in words]
            
            # 写入标签
            for idx, (word, features) in enumerate(zip(words, features_list)):
              word = word.strip()
              if len(word) == 0:
                continue
              if concept.label != unknow_label:
                label = 'B_' + concept.label if idx == 0 else 'I_' + concept.label
              else:
                label = 'O_' + concept.label
              s = "%s\t%s\t%s\n" % (convert_space(word), '\t'.join(features), label)
              fout.write(s)
        else:
          features = fex.get_word_features(concept.word, product_words, brand_words)
          s = '%s\t%s\t%s\n' % (convert_space(concept.word), '\t'.join(features), concept.label)
          fout.write(s)
      fout.write('\n')
    except Exception, e:
      print 'convert2bio_word failed  e=%s' % (e)

def get_note_concept_label(name, notes, concept_labels, match_word_seg):
  '''
  return : list of cut pos tag
  '''
  # 初始化分词
  if match_word_seg:
    cut_list = g_bgnseg.cut(name)
    cut_pos_tag_list = ['I' for x in range(len(name))]
    pos = 0
    for word in cut_list:
      if len(word) == 0:
        continue
      cut_pos_tag_list[pos] = 'B'
      pos += len(word)
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
      # 分词边界检测
      if match_word_seg:
        if cut_pos_tag_list[start] != 'B' or \
           (end < len(name) and cut_pos_tag_list[end] != 'B'):
          continue
      assert(start >= 0 and start < end and end <= len(name))
      concept_labels[start] = ConceptLabel(start, end, name[start:end], label)
  # 返回
  if match_word_seg:
    return cut_pos_tag_list
  else:
    return None

def convert2bio_word_with_cut(note_file, bio_file, feature_vocab_list, seg_word_only=True):
  '''
  使用分词来作为词性标注的边界
  '''
  # 加载资源文件
  vocabs_list = list()
  for name, path in feature_vocab_list:
    vocabs = util.load_vocab(path)
    vocabs_list.append(vocabs)
  
  unknow_label = 'UNK'
  fin = codecs.open(note_file, 'r', 'utf8')
  fout = codecs.open(bio_file, 'w', 'utf8')

  for line in fin:
    try:
      items = line.strip().split('\t')
      raw_name = items[2]      
      notes = json.loads(items[3])
      
      # 处理标注部分
      concept_labels = dict()
      cut_pos_tag_list = get_note_concept_label(raw_name, notes, concept_labels, True)

      # 处理未标注部分
      start, end = 0, 0
      while end < len(raw_name):
        if end not in concept_labels:
          end += 1
        else:
          if start < end:
            concept_labels[start] = ConceptLabel(start, end, raw_name[start:end], unknow_label)
          start = concept_labels[end].end
          end = start
      # 处理最后一个concept
      if start < end:
        concept_labels[start] = ConceptLabel(start, end, raw_name[start:end], unknow_label)
      
      # 作为list处理
      concept_labels = concept_labels.items()
      concept_labels.sort(key=lambda x:x[0])

      # 开始处理各个concept
      for _, concept in concept_labels:
        words = list()
        start = concept.start
        for x in range(concept.start, concept.end):
          if cut_pos_tag_list[x] == 'B':
            if x > start:
              words.append(raw_name[start:x])
            start = x
        if start != concept.end:
          words.append(raw_name[start:concept.end])
        features_list = [fex.get_word_features(word, other_vocab_list=vocabs_list) for word in words]
        features_list = [feature for feature in features_list if feature]
        # 写入标签
        for idx, features in enumerate(features_list):
          if concept.label != unknow_label:
            label = 'SEG' if seg_word_only else concept.label
            label = 'B_' + label if idx == 0 else 'I_' + label
          else:
            label = 'O_' + concept.label
          s = "%s\t%s\n" % ('\t'.join(features), label)
          fout.write(s)
      fout.write('\n')
    except Exception, e:
      print 'convert2bio_word_with_seg failed  e=%s' % (e)

if __name__ == '__main__':
  note_file = sys.argv[1]
  bio_file = sys.argv[2]
  
  #convert2bio_word(note_file, bio_file, product_word_file, brand_word_file)
  #convert2bio_char(note_file, bio_file)
  convert2bio_word_with_cut(note_file, bio_file, config.g_feature_vocab_list, False)


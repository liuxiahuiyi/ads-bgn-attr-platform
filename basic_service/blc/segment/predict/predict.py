#coding=utf8
#author=zhangyunfei
import sys
import json
import yaml
sys.path.append('./')
import config as cfg
import feature_extractor as fex
from util import load_vocab
from util import do_load_label_blocks as do_load_label_blocks
from model_factory import create_model
from load_data import load_vocs, get_data

g_config_file = './config.yml'
with open(g_config_file) as file_config:
  g_config = yaml.load(file_config)
g_brand_file = './brand_word.dat'
g_product_file = './product_word.dat'
g_brand_vocabs = load_vocab(g_brand_file)
g_product_vocabs = load_vocab(g_product_file)
g_label_voc = dict()
g_batch_size = 1024

def load_model(config_file):
  '''
  return : model, vocs
  '''
  with open(g_config_file) as file_config:
    config = yaml.load(file_config)

  feature_names = config['model_params']['feature_names']
  use_char_feature = config['model_params']['use_char_feature']
  
  # 加载vocs
  path_vocs = []
  if use_char_feature:
    path_vocs.append(config['data_params']['voc_params']['char']['path'])
  for feature_name in feature_names:
    path_vocs.append(config['data_params']['voc_params'][feature_name]['path'])
  path_vocs.append(config['data_params']['voc_params']['label']['path'])
  vocs = load_vocs(path_vocs)
  
  # 加载模型
  model = create_model(config_file, True)
  return model, vocs

g_model, g_vocs = load_model(g_config_file)
for key in g_vocs[-1]:
  g_label_voc[g_vocs[-1][key]] = key

def predict_helper(instances, data_dict):
  '''
  return : list of label instance
  '''
  unknow_label = 'UNK'
  instance_labels = list()
  viterbi_sequences = g_model.predict_silence(data_dict)
  for i, items in enumerate(instances):
    labels = list()
    for j, item in enumerate(items):
      if j < len(viterbi_sequences[i]) and viterbi_sequences[i][j] in g_label_voc:
        labels.append(g_label_voc[viterbi_sequences[i][j]])
      else:
        labels.append(unknow_label)
    instance_labels.append(labels)
  return instance_labels

# 1. token分割
# 2. 特征抽取
def get_bio_matrix(sku_seg_list):
  '''
  sku_seg_list : list of segment
  return : list of feature_tokens
  feautre_tokens : list of features
  '''
  # 特征抽取
  feature_tokens = list()
  for token in sku_seg_list:
    features = fex.get_word_features(token, other_vocab_list=cfg.g_feature_vocab_list)
    if not features:
      continue
    feature_tokens.append(features)
  
  return feature_tokens
    
def pack_output(skuid, skuname, instance, labels):
  token_labels = list()
  token_start = list()
  start = 0
  for feature_tokens, label in zip(instance, labels):
    word = feature_tokens[0]
    token_labels.append((word, label))
    token_start.append(start)
    start += len(word)
  # 按照token分割标签
  json_output = list()
  label_blocks = do_load_label_blocks(token_labels, True)
  for block in label_blocks:
    if (not block.label) or (block.label == 'UNK'):
      continue
    start_token = block.pos
    end_token = block.pos + block.length
    data = {'start':token_start[start_token],
            'end':token_start[end_token] if end_token < len(token_start) else token_start[-1],
            'label':block.label.encode('utf8'),
            'word':block.word.encode('utf8')}
    json_output.append(data)
  json_output = json.dumps(json_output, ensure_ascii=False)
  s = '%s\t%s\t%s' % (skuid, skuname, json_output)
  return s

def handle_batch(instance_infos):
  '''
  instance_infos : list instance_info
  instance : (skuid, skuname, sku_seg_list)
  '''
  instances, infos = list(), list()
  for skuid, skuname, sku_seg_list in instance_infos:
    infos.append((skuid, skuname))
    instances.append(get_bio_matrix(sku_seg_list))
  data_dict = get_data(sentences=instances, 
                       feature_names=g_config['model_params']['feature_names'],
                       vocs=g_vocs, 
                       max_len=g_config['model_params']['sequence_length'],
                       model='test',
                       use_char_feature=g_config['model_params']['use_char_feature'], 
                       word_len=g_config['model_params']['word_length'], 
                       trace=False)
  instance_labels = predict_helper(instances, data_dict)
  # print label
  for (skuid, skuname), instance, labels in zip(infos, instances, instance_labels):
    string = pack_output(skuid, skuname, instance, labels)
    print string

def Map():
#  sep = '\001'
  sep = '\t'
  instance_infos = list()
  for line in sys.stdin:
    try:
      skuid, skuname, seg_info = line.strip().split(sep)[:3]
      seg_info = json.loads(seg_info)
      instance_infos.append((skuid, skuname, seg_info))
      if len(instance_infos) >= g_batch_size:
        handle_batch(instance_infos)
        instance_infos = list()
    except Exception, e:
      pass
  try:
    handle_batch(instance_infos)
  except Exception, e:
    pass

def Reduce():
  pass

if __name__ == '__main__':
  cmd = sys.argv[1]
  globals()[cmd]()

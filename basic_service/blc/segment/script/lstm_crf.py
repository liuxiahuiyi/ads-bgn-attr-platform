#coding=utf8

import sys
import os
import yaml
import codecs

def get_dimession(train_file):
  f = open(train_file, 'r')
  line = f.readline()
  dim = len(line.strip().split('\t')) - 1
  assert(dim >= 0)
  return dim

if __name__ == '__main__':
  config_file = sys.argv[1]
  train_file = sys.argv[2]
  test_file = sys.argv[3]
  pred_file = sys.argv[4]
  
  with open(config_file) as file_config:
    config = yaml.load(file_config)
  
  config['data_params']['path_train'] = os.path.abspath(train_file)
  config['data_params']['path_test'] = os.path.abspath(test_file)
  config['data_params']['path_result'] = os.path.abspath(pred_file)
  
  # add feature config
  feature_dim = get_dimession(train_file)
  feature_names = list()
  config['model_params']['embed_params'] = {'char':config['model_params']['embed_params']['char']}
  for i in range(feature_dim):
    name = 'f%s' % i
    config['data_params']['voc_params'][name] = {'min_count': 0, 'path': './Res/voc/%s.voc.pkl' % name}
    config['model_params']['embed_params'][name] = {'dropout_rate': 0.3,
                                                    'path': None,
                                                    'path_pre_train': None,
                                                    'shape': [0, 32]
                                                    }
    feature_names.append(name)
  config['model_params']['feature_names'] = feature_names
  
  with codecs.open(config_file, 'w', encoding='utf-8') as file_w:
    yaml.dump(config, file_w)


#coding=utf8
#author=zhangyunfei

import codecs
import yaml
import pickle
import tensorflow as tf
from load_data import load_vocs 
from model import SequenceLabelingModel

def create_model(config_file, restore):
    # 加载配置文件
    with open(config_file) as file_config:
        config = yaml.load(file_config)

    feature_names = config['model_params']['feature_names']
    use_char_feature = config['model_params']['use_char_feature']

    # 初始化embedding shape, dropouts, 预训练的embedding也在这里初始化)
    feature_weight_shape_dict, feature_weight_dropout_dict, \
        feature_init_weight_dict = dict(), dict(), dict()
    for feature_name in feature_names:
        feature_weight_shape_dict[feature_name] = \
            config['model_params']['embed_params'][feature_name]['shape']
        feature_weight_dropout_dict[feature_name] = \
            config['model_params']['embed_params'][feature_name]['dropout_rate']
        path_pre_train = config['model_params']['embed_params'][feature_name]['path']
        if path_pre_train:
            with open(path_pre_train, 'rb') as file_r:
                feature_init_weight_dict[feature_name] = pickle.load(file_r)
    # char embedding shape
    if use_char_feature:
        feature_weight_shape_dict['char'] = \
            config['model_params']['embed_params']['char']['shape']
        conv_filter_len_list = config['model_params']['conv_filter_len_list']
        conv_filter_size_list = config['model_params']['conv_filter_size_list']
    else:
        conv_filter_len_list = None
        conv_filter_size_list = None

    # 加载模型
    model = SequenceLabelingModel(
        sequence_length=config['model_params']['sequence_length'],
        nb_classes=config['model_params']['nb_classes'],
        nb_hidden=config['model_params']['bilstm_params']['num_units'],
        num_layers=config['model_params']['bilstm_params']['num_layers'],
        feature_weight_shape_dict=feature_weight_shape_dict,
        feature_init_weight_dict=feature_init_weight_dict,
        feature_weight_dropout_dict=feature_weight_dropout_dict,
        dropout_rate=config['model_params']['dropout_rate'],
        nb_epoch=config['model_params']['nb_epoch'], 
        feature_names=feature_names,
        batch_size=config['model_params']['batch_size'],
        train_max_patience=config['model_params']['max_patience'],
        use_crf=config['model_params']['use_crf'],
        l2_rate=config['model_params']['l2_rate'],
        rnn_unit=config['model_params']['rnn_unit'],
        learning_rate=config['model_params']['learning_rate'],
        use_char_feature=use_char_feature,
        conv_filter_size_list=conv_filter_size_list,
        conv_filter_len_list=conv_filter_len_list,
        word_length=config['model_params']['word_length'],
        path_model=config['model_params']['path_model'])
    if restore:
      saver = tf.train.Saver()
      saver.restore(model.sess, config['model_params']['path_model'])
    return model

if __name__ == '__main__':
  pass


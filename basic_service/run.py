# -*- encoding:utf-8 -*-
from platforms import PLATFORM
import os
# import tensorflow as tf
class CONFIG:
    def __init__(self,com_attr_cd):
        # second level category
        self.item_second_cate_cd = "1346"
        # index process com_attr_cd
        self.com_attr_cd = com_attr_cd
        # 1:train model,0:skip train model
        self.train_flag = 1
        # model type: rnn,cnn+mlp,cnn:cnn+cosin
        self.model_type = "rnn"
        # train data process dt
        self.train_data_date = ["2018-06-01","2018-03-01","2017-12-01","2017-09-01"]
        # 1:train model,0:not train model
        self.model_flag = 1
        # max sequence length
        self.max_seq = 25
        # model learning rate
        self.learning_rate = 0.01
        # gpu id
        self.gpu_id = "0"
        # train steps
        self.train_steps = 100000
        # batch size
        self.batch_size = 100
        # print loss every
        self.print_every = 100
        # step frequency in order to evaluate the model using a evaluate set
        self.evaluate_every = 1000
        # track max history accuracy
        self.track_history = 20
        # drop out 
        self.dropout_prob = 0.5
        # hidden layer of rnn
        self.hidden_size = [100,100,100]
        # word2vec dim
        # every day get numbers of data for train word2vec
        self.w2v_number_per_day = 1000000
        # 
        self.w2v_file = '../data/' + self.item_second_cate_cd + '_vec.txt'
        self.w2v_orig_file = '../data/' + self.item_second_cate_cd + '_w2v_orig.txt'
        self.index_path = '../data/' + self.item_second_cate_cd + '_' + self.com_attr_cd
        self.train_file = self.index_path + '/train.txt'
        self.orig_tr_file = self.index_path + '/orig_tr.txt'
        self.val_file = self.index_path + '/val.txt'
        self.dic_save_path = '../data/dic.txt'
        if not os.path.exists(self.index_path):
            os.makedirs(self.index_path)
def main():
    config = CONFIG("872")

    model = PLATFORM(config)

    # get word2vec data by put get_w2v_data.sh on bi_tao
    # run it ,sh get_w2v_data.sh item_second_cate_cd date
    # 
    model.process_w2v(config.w2v_orig_file,config.w2v_file)

    # get train model data,and process
    # put get_train_data.sh and get_train_data_orig.sql on bi_tao, \
    # run it "sh get_train_data.sh com_attr_cd item_second_cate_cd 2018-06-01(data date)"
    model.process_train_data(config.orig_tr_file,config.train_file,config.val_file, \
        config.dic_save_path,config.com_attr_cd,config.item_second_cate_cd)
    # train model
    # model.train_model(config.train_file,config.val_file,len(map_tags),config.w2v_file)
    model.train_model(config.train_file,config.val_file,config.dic_save_path,config.w2v_file)

    # predict model
    model.predict_data(config.dic_save_path)

    # get run on hadoop-streaming program 
    # pack program at './' + self.config.item_second_cate_cd + '_' + self.config.com_attr_cd + '_streaming'
    # model.pack_program()

    # run in 

if __name__=='__main__':
    main()
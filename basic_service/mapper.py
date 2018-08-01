# -*- encoding:utf-8 -*-
from platforms import PLATFORM
from data_manager import DataManager
import tensorflow as tf
import time
FLAGS = tf.app.flags.FLAGS
tf.app.flags.DEFINE_string('item_second_cate_cd', "",'item_second_cate_cd id')
tf.app.flags.DEFINE_string('com_attr_cd', "",'com_attr_cd id')
tf.app.flags.DEFINE_string('dt', "",'date')
tf.app.flags.DEFINE_string('model', "",'best_model_path')
class CONFIG:
    def __init__(self,com_attr_cd,item_second_cate_cd,model_path):
        # second level category
        self.item_second_cate_cd = item_second_cate_cd
        # index process com_attr_cd
        self.com_attr_cd = com_attr_cd
        # vec path
        self.w2v_path = './program/' + self.item_second_cate_cd + '_' + 'vec.txt'
        # model path
        self.model_path = model_path
        # max_seq
        self.max_seq = 25
        # model_type
        self.model_type = "rnn"
        # hidden_size
        self.hidden_size = [100]
        # dropout_prob
        self.dropout_prob = 1.0
        # batch_size
        self.batch_size = 100
def main():
    config = CONFIG(FLAGS.com_attr_cd,FLAGS.item_second_cate_cd,FLAGS.model)
    data_manager = DataManager(True)
    model = PLATFORM(config,True)

    # dic of number to classes
    map_number_classes = data_manager.get_dic('./program/dic.txt',config.com_attr_cd,config.item_second_cate_cd)
    # process orig data
    t = time.time()
    write_file = str(int(round(t * 1000))) + '.csv'
    orig_data = data_manager.process_test_data_all(config.item_second_cate_cd,write_file,config.com_attr_cd,map_number_classes)
    # run predict program 
    predict_result = model.predict_data('./program/dic.txt',write_file,config.w2v_path,"",True,config.model_path)
    # print result
    map_com_attr={'1342':{'6536':u'基础风格','3497':u'适用人群','3298':u'版型','2443':u'厚度','3299':u'衣长','2882':u'腰型','2879':u'衣门襟'}, \
    '1346':{'872':u'风格','3237':u'材质','3497':u'适用人群'}}
    map_second_cate_name = {'1342':u'男装','1346':u'配件'}
    index_second_cate_name = map_second_cate_name[config.item_second_cate_cd]

    data_manager.print_predict(predict_result,orig_data,map_number_classes, \
        config.com_attr_cd,config.item_second_cate_cd,FLAGS.dt, \
        map_com_attr[config.item_second_cate_cd],index_second_cate_name)
if __name__=='__main__':
    main()

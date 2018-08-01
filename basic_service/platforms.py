# -*- encoding:utf-8 -*-
import os
import tensorflow as tf
from rnn import Model as RNN
import pandas as pd
import numpy as np
from data_manager import DataManager
from word2vec import Word2Vec
class PLATFORM:
    def __init__(self, config,online_flag=False):
        self.config = config
        # 
        if not online_flag:
            os.environ["CUDA_VISIBLE_DEVICES"] = self.config.gpu_id
            self.index_path = '../data/' + self.config.item_second_cate_cd + \
                                '_' + self.config.com_attr_cd
            if not os.path.exists(self.index_path):
                os.makedirs(self.index_path)
        self.data_manager = DataManager(online_flag)
        self.Word2Vec = Word2Vec('../data',self.config.item_second_cate_cd,online_flag)
    def train_model(self,train_file,val_file,dic_path,w2v_file):
        if self.config.train_flag:
            print ("run train model")
            # step 1 get word2vec data and train it
            # self.w2v_file = self.get_w2v()
            # # step 2 get train data from hive
            # self.get_train_data()
            # # step 3 process train data
            # self.train_file,self.val_file,self.n_class = self.process_train_data()
            # step 4 read w2v and train data,prepare to train model
            
            # self.w2v_file = '../data/' + self.config.item_second_cate_cd + '_vec.txt'
            # self.val_file = self.index_path + '/val.txt'
            # self.train_file = self.index_path + '/train.txt'
            # self.n_class = 3

            self.map_tags = self.data_manager.get_dic(dic_path,self.config.com_attr_cd, \
                self.config.item_second_cate_cd)
            
            self.w2v_file = w2v_file
            self.val_file = val_file
            self.train_file = train_file
            self.n_class = len(self.map_tags)
            graph = tf.Graph()
            with graph.as_default():
                # step 5 train model
                tr_x,tr_y,seq_length_tr,val_x,val_y,seq_length = self.read_date(self.val_file,self.train_file)
                self.train_model_batch(tr_x,tr_y,seq_length_tr,val_x,val_y,seq_length,graph)
        else:
            print ("skip train model")
    def predict_data(self,dic_path,test_file="",w2v_file="",write_file="",online_flag=False,best_model_path=""):
        if test_file == "":
            self.test_file = self.index_path + '/val.txt'
        else:
            self.test_file = test_file
        if w2v_file == "":
            self.w2v_file = '../data/' + self.config.item_second_cate_cd + '_vec.txt'
        else:
            self.w2v_file = w2v_file
        if write_file == "":
            self.test_write_file = self.index_path + '/val_result.txt'
        else:
            self.test_write_file = ""
        self.map_tags = self.data_manager.get_dic(dic_path,self.config.com_attr_cd, \
                self.config.item_second_cate_cd)
        self.n_class = len(self.map_tags)
        graph = tf.Graph()
        with graph.as_default():
            # step 1. get predict data from hive
            _1,_2,_3,val_x,val_y,seq_length = self.read_date(self.test_file)
            # step 2. run predict data by hadoop streaming
            result = self.process_predict(val_x,val_y,seq_length,graph,best_model_path)
        if not online_flag:
            fw = open(self.test_write_file,'w')
            fw.write(result)
            fw.close()
        if online_flag:
            return result
    def process_w2v(self,orig_file,vec_file,word_min_count=10):
        self.Word2Vec.run_w2v(orig_file,vec_file,word_min_count)
    def pack_program(self):
        streaming_work_path = './' + self.config.item_second_cate_cd + \
            '_' + self.config.com_attr_cd + '_streaming'
        program_path = streaming_work_path + '/program'
        checkpoints_path = program_path + '/checkpoints'
        best_model_path = self.index_path + '/checkpoints/checkpoints_'  + \
            self.config.item_second_cate_cd + '_' + \
            self.config.com_attr_cd
        if not os.path.exists(streaming_work_path):
            os.makedirs(streaming_work_path)
        if not os.path.exists(program_path):
            os.makedirs(program_path)
        os.system('cp ./platforms.py ' + program_path)
        os.system('cp ./mapper.py ' + program_path)
        os.system('cp ./data_manager.py ' + program_path)
        os.system('cp ./reducer.py ' + program_path)
        os.system('cp ./rnn.py ' + program_path)
        os.system('cp ./word2vec.py ' + program_path)
        os.system('cp ./run_streaming.sh ' + streaming_work_path + '/run.sh')
        os.system('cp ./generate_sql.sh ' + streaming_work_path)
        os.system('cp  ./get_data_all_orig.sql ' + streaming_work_path)
        os.system('cp  ' + self.config.dic_save_path + ' ' + program_path)
        os.system('cp  ' + self.config.w2v_file + ' ' + program_path)
        os.system('cp -r ./blc ' + program_path)
        os.system('cp -r ' + best_model_path + ' ' + checkpoints_path)


    def process_train_data(self,read_file,write_file_tr,write_file_val, \
        dic_save_path,com_cd,index_second_cate,title_contain_value_mode=1,save_others=False,only_get_1_tag=True,
        remove_multiple_tags=True,index_remove_brand=False,train_model=True,tags_limit=0.01):
        map_tags = self.data_manager.process_train_data(read_file,write_file_tr, \
            write_file_val,dic_save_path,com_cd,index_second_cate,title_contain_value_mode,save_others,only_get_1_tag, \
            remove_multiple_tags,index_remove_brand,train_model,tags_limit)
        return map_tags
    def process_predict(self,ind_x,ind_y,seq_length,graph,best_model_path):
        # get model and init
        self.model = self.init_model()
        if best_model_path == "":
            self.best_model_path = self.index_path + '/checkpoints/checkpoints_'  + \
                self.config.item_second_cate_cd + '_' + \
                self.config.com_attr_cd + '/best_model'
        else:
            self.best_model_path = best_model_path
        print ('load model from: ' + self.best_model_path)

        # tensorflow session
        test_unary_score = self.test_unary_score()

        sv = tf.train.Supervisor(graph=graph, logdir="")

        with sv.managed_session(master='') as sess:
            sv.saver.restore(sess, self.best_model_path)
            # acc,result = test_evaluate(sess,test_unary_score,ind_x,ind_y,"test")
            acc,result = self.test_evaluate(sess,test_unary_score,self.inp,ind_x,ind_y,self.seq_length,seq_length,True)
        return result
    def process_orig_data_2_tr_te(train_model=True):
        pass
    def get_w2v(self):
        # step 1. get data 
        self.get_w2v_data()
        # step 2. process original data
        self.process_w2v_data()
        # step 2. run w2v
        self.run_w2v()
    def get_w2v_data(self):
        sql_content = ""
        for cd in self.config.item_second_cate_cd:
            for dt in self.config.item_second_cate_cd:
                sql += "hive -e \"select sku_name from gdm.gdm_m03_item_sku_da" + \
                "a where item_second_cate_cd = \'" + cd + "\'  and dt = \'" + dt + \
                "\' and sku_valid_flag = 1 and sku_status_cd != '3000' and" + \
                "sku_status_cd != '3010' and item_sku_id is not null order by rand() limit " \
                + str(self.config.w2v_number_per_day) + "\" >> ../data/w2v_title.txt\n"
        print "get w2v sql : ",sql_content
        os.system(sql_content)
    def process_w2v_data():
        print "test"
    def run_w2v():
        print "test"
    def get_train_data(self):
        print "test"
    # def init_train():  
    def train_model_batch(self,tr_x,tr_y,seq_length_tr,val_x,val_y,seq_length,graph):
        checkpoints_best_work = self.index_path + '/checkpoints/checkpoints_'  + \
            self.config.item_second_cate_cd + '_' + self.config.com_attr_cd
        if not os.path.exists(checkpoints_best_work):
            os.makedirs(checkpoints_best_work)
        print ("train_model_batch begin")
        print ("tr_x, tr_y,seq_length_tr:",tr_x.get_shape(),tr_y.get_shape(),seq_length_tr.get_shape())
        # input_ids = tf.placeholder(dtype=tf.int32, shape=[None])
        # graph = tf.Graph()
        # with graph.as_default():
            # get model and init
        self.model = self.init_model()
        # input_embedding = tf.nn.embedding_lookup(self.words, input_ids)
        # word_vectors = tf.nn.embedding_lookup(self.words, tr_x)
        # loss function
        total_loss,accuracy = self.loss(tr_x, tr_y, seq_length_tr)
        # learn rate and 
        train_op = self.train_step(total_loss)

        # # tensorflow session
        # sess = tf.Session()
        # init
        # sess.run(tf.global_variables_initializer())
        # model saver
        # saver = tf.train.Saver()
        # best model result
        best_acc = 0.0
        # 
        test_unary_score = self.test_unary_score(trainMode=True)
        sv = tf.train.Supervisor(graph=graph, logdir="")
        best_model_path = checkpoints_best_work + '/best_model'
        with sv.managed_session(master='') as sess:
            # run train model
            for i in range(self.config.train_steps):
                _ = sess.run(train_op)
                if (i + 1) % self.config.print_every == 0:
                    print ("accuracy:",sess.run(accuracy))
                    print("[%d] loss: [%r]" % (i + 1, sess.run(total_loss)))
                if (i + 1) % self.config.evaluate_every == 0:
                    acc,result = self.test_evaluate(sess,test_unary_score,self.inp,val_x,val_y,self.seq_length,seq_length)
                    if acc > best_acc:
                        if i:
                            sv.saver.save(sess, best_model_path)
                            print ("model save : ",best_model_path)
                        best_acc = acc
                        trackHist = 0
                    elif trackHist > self.config.track_history:
                        print("always not good enough in last %d histories, best \
                            accuracy:%.3f" % (trackHist, best_acc))
                        break
                    else:
                        trackHist += 1
                    sv.saver.save(sess, self.index_path + '/checkpoints/always_model/model')
    def init_model(self):
        # word2vec variable
        self.words = tf.Variable(self.c2v, name="words")
        print ("words:",self.words.get_shape())
        # input placeholder
        self.inp = tf.placeholder(tf.int32, [None, self.config.max_seq], name='input_x')
        # sequence real lenth
        self.seq_length = tf.placeholder(tf.int32, [None], name='seq_length')
        # choose model
        index_model = self.choose_model()
        return index_model
    def inference(self, X, seq_length,trainMode=True,reuse=False):
        word_vectors = tf.nn.embedding_lookup(self.words, X)
        # print ("word_vectors,",word_vectors.get_shape())
        # reuse = False if trainMode else True
        unary_scores = self.model.scores(word_vectors,seq_length, train_model=trainMode, reuse=reuse)
        # if trainMode:
        #     self.rnn_output = rnn_output
        #     self.word_vectors = word_vectors
        return unary_scores
    def test_unary_score(self,trainMode=False):
        # P = self.inference(self.inp,self.seq_length,trainMode=False)
        reuse = True if trainMode else False
        P = self.inference(self.inp,self.seq_length,trainMode=trainMode,reuse=reuse)
        softmax = tf.nn.softmax(P, name='predictions')
        # predict_res = tf.argmax(softmax, 1)
        return softmax
    def loss(self, X, Y, seq_length):
        index_score = self.inference(X, seq_length, trainMode=True)
        print ("index_score",index_score.get_shape())
        cross_entropy = tf.nn.softmax_cross_entropy_with_logits(logits=index_score, labels=Y)
        print ("cross_entropy:",cross_entropy.get_shape())
        loss = tf.reduce_mean(cross_entropy)
        correct_pred = tf.equal(tf.argmax(tf.nn.softmax(index_score), 1), tf.argmax(Y, 1))
        accuracy = tf.reduce_mean(tf.cast(correct_pred, tf.float32), name='accuracy')
        return loss,accuracy
    # model predict result
    # need change 
    def test_evaluate(self,sess,unary_score,inp,tX, tY,seq_place,seq_length,write_flag=False):
        totalEqual = 0
        batchSize = self.config.batch_size
        totalLen = tX.shape[0]
        numBatch = int((tX.shape[0] - 1) / batchSize) + 1
        correct_labels = 0
        total_labels = 0
        content = ""
        for i in range(numBatch):
            endOff = (i + 1) * batchSize
            if endOff > totalLen:
                endOff = totalLen
            y = tY[i * batchSize:endOff]
            seq = seq_length[i * batchSize:endOff]
            feed_dict = {inp: tX[i * batchSize:endOff],seq_place: seq}
            # softmax = tf.nn.softmax(unary_score, name='predictions')
            predict_res = sess.run([unary_score], feed_dict)
            # print (predict_res)
            # print (predict_res[0])
            for predict_res_, y_ in zip(
                predict_res[0], y):
                # print (predict_res_, y_)
                predict_i,y_i = np.argmax(predict_res_),np.argmax(y_)
                total_labels += 1
                if predict_i == y_i:
                    correct_labels += 1
                if write_flag:
                    content += ",".join(str(i) for i in predict_res_) + ';' + \
                    str(predict_i) + ',' + str(y_i) + '\n'
            # correct_pred = tf.equal(predict_res, tf.argmax(tY, 1))
            # accuracy = tf.reduce_mean(tf.cast(correct_pred, tf.float32), name='accuracy')

        accuracy = 100.0 * correct_labels / float(total_labels)
        print("Accuracy: %.3f%%" % accuracy)
        return accuracy,content
    def train_step(self,total_loss):
        # return tf.train.AdamOptimizer(self.config.learning_rate).minimize(total_loss)
        return tf.train.RMSPropOptimizer(self.config.learning_rate).minimize(total_loss)
    def choose_model(self):
        if self.config.model_type == "rnn":
            return RNN(self.config.hidden_size,self.config.max_seq,self.n_class,self.config.dropout_prob)
        elif self.config.model_type == "cnn":
            return CNN()
    def read_date(self,val_file,train_file=""):
        print "read date begin"
        # read word2vec 
        self.c2v,self.map_w2v = self.read_w2v()
        # read test data
        val_x,val_y,seq_length = self.read_val(val_file)
        # read train data
        if train_file == "":
            tr_x,tr_y,seq_length_tr = [],[],[]
        else:
            tr_x,tr_y,seq_length_tr = self.read_train(train_file)
        # all result
        return tr_x,tr_y,seq_length_tr,val_x,val_y,seq_length
    def read_val(self,index_file):
        # get val data
        x,y,_f,seq_length = self.read_csv(index_file)
        return x,y,seq_length
    def read_train(self,index_file):
        # get val data
        _x,_y,pre_file,_ = self.read_csv(index_file,True)
        print ("pre_file",pre_file)
        whole = self.read_csv_data(self.config.batch_size, pre_file)
        label = tf.transpose(tf.stack(whole[0:self.n_class]))
        seq_length = tf.transpose(tf.stack(whole[self.n_class]))
        features = tf.transpose(tf.stack(whole[self.n_class + 1:]))
        return features, label, seq_length
    def read_csv_data(self,batch_size, file_name):
        filename_queue = tf.train.string_input_producer([file_name])
        reader = tf.TextLineReader(skip_header_lines=0)
        key, value = reader.read(filename_queue)
        # decode_csv will convert a Tensor from type string (the text line) in
        # a tuple of tensor columns with the specified defaults, which also 
        # sets the data type for each column
        decoded = tf.decode_csv(
            value,
            field_delim=' ',
            record_defaults=[[0] for i in range(self.config.max_seq + self.n_class + 1)])

        # batch actually reads the file and loads "batch_size" rows in a single
        # tensor
        return tf.train.shuffle_batch(decoded,
                                  batch_size=self.config.batch_size,
                                  capacity=self.config.batch_size * 50,
                                  min_after_dequeue=self.config.batch_size)
    def read_csv(self,index_file,write_flag=False):
        data = pd.read_csv(index_file, nrows=None)
        index_sentiments = np.squeeze(data.as_matrix(columns=['Sentiment']))
        index_sentiments = np.eye(self.n_class)[index_sentiments]
        samples = data.as_matrix(columns=['SentimentText'])[:, 0]
        sample_lengths = []
        tensors = []
        row_number = 0
        word_count = 1
        write_file = self.index_path + '/train_batch_data.txt'
        if write_flag:
            content_write = ''
            fs = open(write_file,'w')
        for sample in samples:
            sample_words = sample.decode('utf8').split(' ')
            encoded_sample = []
            ii = -1
            for word in sample_words:  # distinct words in list
                if self.map_w2v.has_key(word):
                #if word in map_w2v:
                    value = self.map_w2v[word]
                else:
                    value = self.map_w2v['<UNK>']
                encoded_sample += [int(value)]
                ii += 1
                if (ii == self.config.max_seq - 1):
                    last_word = sample_words[-1]
                    if self.map_w2v.has_key(last_word):
                        last_value = self.map_w2v[last_word]
                    else:
                        last_value = self.map_w2v['<UNK>']
                    encoded_sample[-1] = int(last_value)
                    break
            if (ii >= self.config.max_seq):
                print("sample over max_seq",sample)
                print("sample over max_seq",ii,self.config.max_seq)
                raise Exception('Error: Provided sequence length is not sufficient')
            index_seq_length = len(encoded_sample)
            for ij in range(ii + 1,self.config.max_seq):
                encoded_sample += [0]
            if write_flag:
                content_write += " ".join(str(int(i)) for i in index_sentiments[row_number]) \
                + ' ' +  str(index_seq_length) + ' ' + " ".join(str(i) for i in encoded_sample) + '\n'
                if row_number % 10000:
                    fs.write(content_write)
                    content_write = ''
            else:
                tensors += [encoded_sample]
                sample_lengths += [index_seq_length]
            row_number += 1

        if write_flag:
            fs.write(content_write)
            fs.close()
        index_length = np.array(sample_lengths)
        _tensors = np.array(tensors)
        return _tensors,index_sentiments,write_file,index_length
    def read_w2v(self):
        fp = open(self.w2v_file)
        print("load data from:", self.w2v_file)
        line = fp.readline().strip().decode('utf8')
        ss = line.split(" ")
        total = int(ss[0])
        dim = int(ss[1])
        ws = []
        mv = [0 for i in range(dim)]
        second = -1
        index_flag = 0
        map_w2v = {}
        for t in range(total):
            if ss[0] == '<UNK>':
                second = t
            line = fp.readline().strip().decode('utf8')
            ss = line.split(" ")
            assert (len(ss) == (dim + 1))
            vals = []
            for i in range(1, dim + 1):
                fv = float(ss[i])
                mv[i - 1] += fv
                vals.append(fv)
            ws.append(vals)
            map_w2v[ss[0]] = str(index_flag)
            index_flag += 1
        for i in range(dim):
            mv[i] = mv[i] / total
        assert (second != -1)
        # append one more token , maybe useless
        ws.append(mv)
        if second != 1:
            t = ws[1]
            ws[1] = ws[second]
            ws[second] = t
        fp.close()
        return np.asarray(ws, dtype=np.float32),map_w2v    
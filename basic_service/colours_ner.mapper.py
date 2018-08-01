# -*- coding: utf-8 -*-
# @Author: Koth Chen
# @Date:   2016-07-26 13:48:32
# @Last Modified by:   Koth
# @Last Modified time: 2017-04-07 23:04:45
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import sys  
reload(sys)
sys.setdefaultencoding('utf-8')
import tensorflow as tf
import os
import json
import re
from rnn import Model as RNN
sys.path.insert(0, 'blc/segment/basic_segmentation')
# from bgnseg import BgnSeg
# bgnseg = BgnSeg()
FLAGS = tf.app.flags.FLAGS

tf.app.flags.DEFINE_string('train_data_path', "newcorpus/2014_train.txt",
                           'Training data dir')
tf.app.flags.DEFINE_string('test_data_path', "newcorpus/2014_test.txt",
                           'Test data dir')
tf.app.flags.DEFINE_string('log_dir', "logs", 'The log  dir')
tf.app.flags.DEFINE_string("word2vec_path", "newcorpus/vec.txt",
                           "the word2vec data path")

tf.app.flags.DEFINE_integer("max_sentence_len", 60,
                            "max num of tokens per query")
tf.app.flags.DEFINE_integer("embedding_size", 50, "embedding size")
tf.app.flags.DEFINE_integer("num_tags", 4, "BMES")
tf.app.flags.DEFINE_integer("num_hidden", 100, "hidden unit number")
tf.app.flags.DEFINE_integer("batch_size", 32, "num example per mini batch")
tf.app.flags.DEFINE_integer("train_steps", 150000, "trainning steps")
tf.app.flags.DEFINE_float("learning_rate", 0.001, "learning rate")
tf.app.flags.DEFINE_bool("use_idcnn", False, "whether use the idcnn")
tf.app.flags.DEFINE_integer("track_history", 6, "track max history accuracy")
tf.app.flags.DEFINE_string("tags_file", "tags_all.txt", "The tags  file")
tf.app.flags.DEFINE_string("tags_result_file", "all_colour_online.txt", "The tags  file")
map_w2v = {}
map_colours = {}
map_colours_result = {}
def do_load_data():
    x = []
    y = []
    content = []
    sku_content = []
    r1 = u'[a-zA-Z0-9]'
    for line_i in sys.stdin:
        datablock = line_i.decode('utf8').replace('\n','').replace('\N','NULL').split(',')
        line = datablock[1].replace(' ','~').replace(u'牛仔','').replace(u'条纹','').replace(u'网红','').replace(u'空白','').replace(u'红心','').replace(u'小红帽','').replace(u'小黄','').replace(u'小黑心','').replace(u'千紫','').replace(u'万红','').replace(u'浅浅','').replace(u'极简主义','').replace(u'Ｃ','').replace(u'Ｚ','')
        line = re.sub(r1, '', line)
        if not line:
            continue
        index_colour = '颜色'
        index_id = '2338'
        sku_content.append(datablock[0] + ',' + index_id + ',' + index_colour + ',' + datablock[6])
        lx = []
        result = ''
        gap_length = 0
        print_index = 0
        if line.find('WKVKING') != -1:
            print_index = 1
        for i in range(len(line) + 1):
            if i < gap_length:
                continue
            if (len(lx) >= int(FLAGS.max_sentence_len)) or (i == len(line)):
                #if print_index:
                #    print (lx)
                #    print (result)
                x.append(get_data(lx,int(FLAGS.max_sentence_len)))
                content.append(result)
                lx = []
                result = ''
                break
            # if map_w2v.has_key(line[i]):
            index_word = line[i]
            # print ("index_word",index_word)
            if judge_char_type(line[i]) == 1:
                j = i
                while((j < len(line)) and (judge_char_type(line[j]) == 1)):
                    j += 1
                # print (line[i],i,j)
                index_word = line[i:j]
                gap_length = j
                # print ("number",index_word)
                # print ("title",line)
            elif judge_char_type(line[i]) == 2:
                j = i
                while((j < len(line)) and (judge_char_type(line[j]) == 2)):
                    j += 1
                index_word = line[i:j]
                gap_length = j
                # print ("char",index_word,gap_length,len(line))
                # print ("title",line)
            else:
                gap_length = i + 1

            if index_word == '~':
                index_word = 'block'
            if (index_word in map_w2v):
                index_value = map_w2v[index_word]
            else:
                index_value = map_w2v['<UNK>']
            # print (index_word,index_value)
            lx.append(int(index_value))
            # print (lx)
            result += index_word
    return np.array(x), content,sku_content
def get_tags_map(dataFile):
    f = open(dataFile, "r")
    index_flag = -1
    map_res = {}
    while True:
        data = f.readline()
        if not data:
            break
        datablock = data.decode('utf8').strip('\n').replace('\r','')
        map_res[datablock] = True
        if (datablock.find(u'色') != -1) and (len(datablock) >= 2):
            #print (datablock.replace(u'色',''))
            map_res[datablock.replace(u'色','')] = True
    f.close()
    return map_res                                                             
def judge_char_type(index_char):
    result = 0
    if index_char >= '0' and index_char <= '9':
        result = 1
    if (index_char >= 'a' and index_char <= 'z') or (index_char >= 'A' and index_char <= 'Z'):
        result = 2
    return result
def get_data(x_data,seq_lenth):
    if len(x_data) == seq_lenth:
        result = x_data
    else:
        for i in range(seq_lenth - len(x_data)):
            x_data.append(0)
        result = x_data
    return result
# class BiLSTM:
#     def __init__(self,
#                  numHidden,
#                  maxSeqLen,
#                  numTags):
#         self.num_hidden = numHidden
#         self.num_tags = numTags
#         self.max_seq_len = maxSeqLen
#         self.W = tf.get_variable(
#             shape=[numHidden * 2, numTags],
#             initializer=tf.contrib.layers.xavier_initializer(),
#             name="weights",
#             regularizer=tf.contrib.layers.l2_regularizer(0.001))
#         self.b = tf.Variable(tf.zeros([numTags], name="bias"))

#     def inference(self, X, length, reuse=False):
#         length_64 = tf.cast(length, tf.int64)
#         with tf.variable_scope("bilstm", reuse=reuse):
#             forward_output, _ = tf.nn.dynamic_rnn(
#                 tf.contrib.rnn.LSTMCell(self.num_hidden,
#                                         reuse=reuse),
#                 X,
#                 dtype=tf.float32,
#                 sequence_length=length,
#                 scope="RNN_forward")
#             backward_output_, _ = tf.nn.dynamic_rnn(
#                 tf.contrib.rnn.LSTMCell(self.num_hidden,
#                                         reuse=reuse),
#                 inputs=tf.reverse_sequence(X,
#                                            length_64,
#                                            seq_dim=1),
#                 dtype=tf.float32,
#                 sequence_length=length,
#                 scope="RNN_backword")

#         backward_output = tf.reverse_sequence(backward_output_,
#                                               length_64,
#                                               seq_dim=1)

#         output = tf.concat([forward_output, backward_output], 2)
#         output = tf.reshape(output, [-1, self.num_hidden * 2])
#         if reuse is None or not reuse:
#             output = tf.nn.dropout(output, 0.5)

#         matricized_unary_scores = tf.matmul(output, self.W) + self.b
#         unary_scores = tf.reshape(
#             matricized_unary_scores,
#             [-1, self.max_seq_len, self.num_tags],
#             name="Reshape_7" if reuse else None)
#         return unary_scores
class Model:
    def __init__(self, embeddingSize, distinctTagNum, c2vPath, numHidden):
        self.embeddingSize = embeddingSize
        self.distinctTagNum = distinctTagNum
        self.numHidden = numHidden
        print ("load_w2v begin-----")
        self.c2v = self.load_w2v('../data/1342_vec.txt', 100)
        print ("load_w2v end-----")
        self.words = tf.Variable(self.c2v, name="words")
        layers = [
            {
                'dilation': 1
            },
            {
                'dilation': 1
            },
            {
                'dilation': 2
            },
        ]
        self.model = RNN([100],25,3,0.5)
        self.trains_params = None
        self.inp = tf.placeholder(tf.int32,
                                  shape=[None, 25],
                                  name="input_placeholder")
        pass

    def length(self, data):
        used = tf.sign(tf.abs(data))
        length = tf.reduce_sum(used, reduction_indices=1)
        length = tf.cast(length, tf.int32)
        return length

    def inference(self, X, reuse=None, trainMode=True):
        word_vectors = tf.nn.embedding_lookup(self.words, X)
        length = self.length(X)
        reuse = False if trainMode else True
        if FLAGS.use_idcnn:
            word_vectors = tf.expand_dims(word_vectors, 1)
            unary_scores = self.model.inference(word_vectors, reuse=reuse)
        else:
            unary_scores = self.model.scores(
                word_vectors, length, reuse=reuse)
        return unary_scores, length

    def loss(self, X, Y):
        P, sequence_length = self.inference(X)
        log_likelihood, self.transition_params = tf.contrib.crf.crf_log_likelihood(
            P, Y, sequence_length)
        loss = tf.reduce_mean(-log_likelihood)
        return loss

    def load_w2v(self, path, expectDim):
        fp = open(path, "rb")
        line = fp.readline().strip().decode('utf8')
        ss = line.split(" ")
        total = int(ss[0])
        dim = int(ss[1])
        assert (dim == expectDim)
        ws = []
        mv = [0 for i in range(dim)]
        second = -1
        index_flag = 0
        for t in range(total):
            if ss[0] == '<UNK>':
                second = t
            line = fp.readline().strip().decode('utf8')
            # print(line)
            ss = line.split(" ")
            assert (len(ss) == (dim + 1))
            vals = []
            for i in range(1, dim + 1):
                fv = float(ss[i])
                mv[i - 1] += fv
                vals.append(fv)
            ws.append(vals)
            # print(ss[0])
            # print(ss[0].decode('utf8'))
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
        return np.asarray(ws, dtype=np.float32)

    def test_unary_score(self):
        P, sequence_length = self.inference(self.inp)
        return P, sequence_length


def read_csv(batch_size, file_name):
    filename_queue = tf.train.string_input_producer([file_name])
    reader = tf.TextLineReader(skip_header_lines=0)
    key, value = reader.read(filename_queue)
    # decode_csv will convert a Tensor from type string (the text line) in
    # a tuple of tensor columns with the specified defaults, which also
    # sets the data type for each column
    decoded = tf.decode_csv(
        value,
        field_delim=' ',
        record_defaults=[[0] for i in range(FLAGS.max_sentence_len * 2)])

    # batch actually reads the file and loads "batch_size" rows in a single
    # tensor
    return tf.train.shuffle_batch(decoded,
                                  batch_size=batch_size,
                                  capacity=batch_size * 50,
                                  min_after_dequeue=batch_size)


def test_evaluate(sess, unary_score, test_sequence_length, transMatrix, inp,
                  tX, tY, sku_content):
    totalEqual = 0
    batchSize = FLAGS.batch_size
    totalLen = tX.shape[0]
    numBatch = int((tX.shape[0] - 1) / batchSize) + 1
    correct_labels = 0
    total_labels = 0
    for i in range(numBatch):
        endOff = (i + 1) * batchSize
        if endOff > totalLen:
            endOff = totalLen
        y = tY[i * batchSize:endOff]
        x = tX[i * batchSize:endOff]
        index_sku = sku_content[i * batchSize:endOff]
        feed_dict = {inp: tX[i * batchSize:endOff]}
        unary_score_val, test_sequence_length_val = sess.run(
            [unary_score, test_sequence_length], feed_dict)
        index_value = 0
        for tf_unary_scores_,y_,x_,index_sku_, sequence_length_ in zip(
                unary_score_val,y,x,index_sku,test_sequence_length_val):
            # print("seg len:%d" % (sequence_length_))
            tf_unary_scores_ = tf_unary_scores_[:sequence_length_]
            # y_ = y_[:sequence_length_]
            viterbi_sequence, _ = tf.contrib.crf.viterbi_decode(
                tf_unary_scores_, transMatrix)
            # Evaluate word-level accuracy.
            #print(viterbi_sequence)
            #print(y_)
            # print_result(viterbi_sequence,tY[index_value],transMatrix)
            print_result(viterbi_sequence,y_,x_,index_sku_)
            index_value += 1
    # accuracy = 100.0 * correct_labels / float(total_labels)
    # print("Accuracy: %.3f%%" % accuracy)
    return 1.0

def print_result(viterbi_sequence,y,t_x,index_sku):
    content = ''
    y = y.replace('@','#').replace('block','@')
    y_i = 0
    print_index = 0
    model_flag = 0
    model_end = 0
    index_colour_block = y.split('@')
    block_length = len(index_colour_block)
    index_judge = ''
    model_res = ''
    pre_model_str = ''
    pre_str = ''
    #for ii in range(block_length):
    #    index_block = index_colour_block[block_length - 1 - ii]
    #    if map_colours.has_key(index_block):
    #        index_judge = index_block
    #        break
    #    else:
    #        find_res = judge_colour(index_block)
    #        if (find_res != '') and (len(find_res) >= len(index_judge)):
    #            index_judge = find_res

    for i in range(len(viterbi_sequence)):
        index_value = y[y_i]
        pre_str = y[y_i - 1] if(y_i) else  ''
        if judge_char_type(index_value) == 1:
            j = y_i
            while((j < len(y)) and (judge_char_type(y[j]) == 1)):
                j += 1
            # print (line[i],i,j)
            index_value = y[y_i:j]
            y_i = j
        elif judge_char_type(index_value) == 2:
            j = y_i
            while((j < len(y)) and (judge_char_type(y[j]) == 2)):
                j += 1
            # print (line[i],i,j)
            index_value = y[y_i:j]
            y_i = j
        else:
            y_i += 1
        add_value = ''
        #if print_index:
            #print(index_value,viterbi_sequence[i])
        if index_value == '@':
            index_value = ' '
        if (viterbi_sequence[i] == 1):
            # content += ' ['
            model_flag = 1
            pre_model_str = pre_str
        content += index_value
        if model_flag and not model_end:
            model_res += index_value
        if (viterbi_sequence[i] == 3):
            # content += '] '
            model_end = 1
        # if model_flag and not model_end:
        #    model_res += index_judge
        # print (content)
        # printf('%s%s', add_value,index_value)
    #print(content)
    model_res = re.sub("[A-Za-z0-9\[\`\~\!\@\#\$\^\&\*\(\)\=\|\{\}\'\:\;\'\,\[\]\.\<\>\/\?\~\！\@\#\\\&\*\%]", "", model_res)
    if (pre_model_str == u'深') or (pre_model_str == u'浅') or (pre_model_str == u'玫'):
        model_res = pre_model_str  + model_res
    model_res_index = content.find(model_res)
    if (model_res_index != -1) and (model_res_index > 1) and (content[model_res_index - 2] == u' ') and (content[model_res_index - 1] >= u"\u4e00" and content[model_res_index - 1] <= u"\u9fa6"):
        add_result = content[model_res_index - 1] + model_res
        if (len(bgnseg.cut(add_result)[0]) > 1):
            model_res = add_result
    #if (model_res_index != -1) and (model_res_index + len(model_res) + 2 < len(content)) and (content[model_res_index + len(model_res) + 2] == u' '):
    #    model_res = model_res + content[model_res_index + len(model_res) + 1]
    #content = content.replace(" ","")
    sku_block = index_sku.split(',')
    #if model_res == '':
        #if sku_block[3] != '' and sku_block[3] != 'NULL' and sku_block[3].find(u'其它') != -1:
        #    model_res = sku_block[3].replace('系','')
        #else:
    #    model_res = 'NULL'
    if (model_res.find(u'上') != -1) or (model_res.find(u'下') != -1) or (model_res.find(u'（') != -1) or (model_res.find(u'）') != -1) or (model_res.find(u' ') != -1)  or model_res.find(u'+') != -1 or (model_res.find(u'-') != -1):
        model_res = model_res.replace(u'上青',u'好青').replace(u'上','').replace(u'下','').replace(u'+','').replace(u'-','').replace(u'系','').replace(u' ','').replace(u'（','').replace(u'）','').replace(u'好青',u'上青')
    remove_block = [u'/',u'_',u'衣',u'件',u'领',u'女',u'男',u'底',u'服',u'装',u'送',u'绒',u'款',u'拼',u'袖',u'帽',u'配',u'【',u'边',u'害怕',u'身',u'可',u'款',u'带',u'加',u'生']
    for ii in remove_block:
        model_res = model_res.replace(ii,'')
    for ii in model_res:
        if (ii < u"\u4e00") or (ii > u"\u9fa6"):
            model_res.replace(ii,'')
    #print(model_res,model_res.find(u'色'),sku_block[0])
    se_index = model_res.find(u'色')
    if (se_index != -1) and se_index < len(model_res) -1:
        if se_index == 0:
            model_res_index = content.find(model_res)
            if (model_res_index > 0) and map_colours.has_key(content[model_res_index - 1]) and content[model_res_index - 1] != u'纯':
                model_res = content[model_res_index - 1]
        else:
            if map_colours.has_key(model_res[se_index - 1]) and model_res[se_index - 1] != u'纯':
                model_res = model_res[se_index - 1]
            #print(se_index,len(model_res),model_res)
            elif model_res[se_index - 1] == u'纯' and map_colours.has_key(model_res[se_index + 1]):
                model_res = model_res[se_index + 1]
        #model_res = model_res.replace(u'色','')
    if len(model_res) > 1 and (model_res[0] == model_res[1]):
        model_res = model_res[0]
    #if (not map_colours_result.has_key(model_res)) and (not map_colours.has_key(model_res)) and len(model_res):
    if (not map_colours_result.has_key(model_res)) and len(model_res):    
        model_res = get_colour_search(model_res)
    if model_res.find(u'色') == -1:
        model_res = model_res +  u'色'
    content = sku_block[0] + ',' + model_res + ',' + sku_block[3] + ',' + content + ',' + u'颜色' + ',' + u'女装' + ',1343,' + sku_block[1] + ',' + sku_block[2]   
    if model_res != u'色':
        print(content)
# def get_colour_search(str_value):
#     result = ''
#     se_index = str_value.find(u'色')
#     if (se_index != -1) and (se_index > 0):
#         for i in range(se_index):
#             index_str = str_value[se_index - i -1]
#             #if map_colours_result.has_key(index_str) or  map_colours.has_key(index_str):
#             if map_colours_result.has_key(index_str):
#                 result = index_str
#                 break
#     str_len = len(str_value)
#     for ii in range(str_len):
#         index_str = str_value[str_len - ii - 1]
#         #if map_colours_result.has_key(index_str) or  map_colours.has_key(index_str):
#         if map_colours_result.has_key(index_str):
#             result = index_str
#             break
#     #print (str_value,result)
#     return result
# def judge_colour(str_value):
#     result = ''
#     for ii in map_colours:
#         if (str_value.find(ii.replace('色','')) != -1) and (len(ii) > len(result)):
#             result = ii
#     return result
# def inputs(path):
#     whole = read_csv(FLAGS.batch_size, path)
#     features = tf.transpose(tf.stack(whole[0:FLAGS.max_sentence_len]))
#     label = tf.transpose(tf.stack(whole[FLAGS.max_sentence_len:]))
#     return features, label


# def train(total_loss):
#     return tf.train.AdamOptimizer(FLAGS.learning_rate).minimize(total_loss)



def length(data):
    used = tf.sign(tf.abs(data))
    length = tf.reduce_sum(used, reduction_indices=1)
    length = tf.cast(length, tf.int32)
    return length

# def loss(self, X, Y):
#     P, sequence_length = self.inference(X)
#     log_likelihood, self.transition_params = tf.contrib.crf.crf_log_likelihood(
#         P, Y, sequence_length)
#     loss = tf.reduce_mean(-log_likelihood)
#     return loss

def load_w2v(path, expectDim):
    fp = open(path, "rb")
    line = fp.readline().strip().decode('utf8')
    ss = line.split(" ")
    total = int(ss[0])
    dim = int(ss[1])
    assert (dim == expectDim)
    ws = []
    mv = [0 for i in range(dim)]
    second = -1
    index_flag = 0
    for t in range(total):
        if ss[0] == '<UNK>':
            second = t
        line = fp.readline().strip().decode('utf8')
        # print(line)
        ss = line.split(" ")
        assert (len(ss) == (dim + 1))
        vals = []
        for i in range(1, dim + 1):
            fv = float(ss[i])
            mv[i - 1] += fv
            vals.append(fv)
        ws.append(vals)
        # print(ss[0])
        # print(ss[0].decode('utf8'))
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
    return np.asarray(ws, dtype=np.float32)

def test_unary_score(inp,words):
    P, sequence_length = inference(word,inp)
    return P, sequence_length
def inference(word,X, reuse=None, trainMode=True):
    word_vectors = tf.nn.embedding_lookup(words, X)
    length = self.length(X)
    reuse = False if trainMode else True
    if FLAGS.use_idcnn:
        word_vectors = tf.expand_dims(word_vectors, 1)
        unary_scores = self.model.inference(word_vectors, reuse=reuse)
    else:
        unary_scores = self.model.scores(
            word_vectors, length, reuse=reuse)
    return unary_scores, length


graph = tf.Graph()
with graph.as_default():
    print ("Model begin---------")
    print ("load_w2v begin-----")
    c2v = load_w2v('../data/1342_vec.txt', 100)
    print ("load_w2v end-----")
    words = tf.Variable(c2v, name="words")
    # layers = [
    #     {
    #         'dilation': 1
    #     },
    #     {
    #         'dilation': 1
    #     },
    #     {
    #         'dilation': 2
    #     },
    # ]
    model = RNN([100],25,3,0.5)
    trains_params = None
    inp = tf.placeholder(tf.int32,shape=[None, 25],name="input_placeholder")
    # model = Model(FLAGS.embedding_size, FLAGS.num_tags,FLAGS.word2vec_path, FLAGS.num_hidden)
    test_unary_score, test_sequence_length = model.test_unary_score(inp,words)
    print ("test_unary_score end---------")
    tX, tY, sku_content = do_load_data()
    #P, sequence_length = model.inference(X)
    #total_loss = model.loss(tX, tY)
    #train_op = train(total_loss)
    # test_unary_score, test_sequence_length = model.test_unary_score()
    sv = tf.train.Supervisor(graph=graph, logdir=FLAGS.log_dir)
    with sv.managed_session(master='') as sess:
        sv.saver.restore(sess,  FLAGS.log_dir + '/best_model')
        #sv.saver.restore(sess,'/export/mart_risk_aof/wangsanpeng/rnn_classification/unified_model_platform/data/1342_3298/checkpoints/best_model')
        #_,trainsMatrix = sess.run([train_op,model.transition_params])
        trainsMatrix=[[0.6633002,2.869724,-5.797926,-2.3183627],[-4.1278906,-14.555054,1.6429015,1.0219132],[-7.42705,-1.864328,0.8448285,0.58220387],[1.1706636,-0.70118487,-2.3770168,-8.725252]]
        #print (trainsMatrix)
        #print (test_unary_score)
        acc = test_evaluate(sess, test_unary_score,
                                        test_sequence_length, trainsMatrix,
                                        model.inp, tX, tY,sku_content)

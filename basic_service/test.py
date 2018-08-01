# -*- encoding:utf-8 -*-
import tensorflow as tf
import os
import pandas as pd
from rnn import Model as RNN
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
max_sentence_len = 25
import numpy as np
index_path = '../data/1342_3298'
def read_csv_train(batch_size, file_name):
    filename_queue = tf.train.string_input_producer([file_name])
    reader = tf.TextLineReader(skip_header_lines=0)
    key, value = reader.read(filename_queue)
    # decode_csv will convert a Tensor from type string (the text line) in
    # a tuple of tensor columns with the specified defaults, which also 
    # sets the data type for each column
    decoded = tf.decode_csv(
        value,
        field_delim=' ',
        record_defaults=[[0] for i in range(25 + 3 + 1)])

    # batch actually reads the file and loads "batch_size" rows in a single
    # tensor
    return tf.train.shuffle_batch(decoded,batch_size=batch_size,capacity=batch_size * 50,min_after_dequeue=batch_size)
def inputs(path):
    whole = read_csv(100, path)
    # whole = read_csv(FLAGS.batch_size, "../nvzhuang100/test.txt")
    features = tf.transpose(tf.stack(whole[0:max_sentence_len]))
    length = tf.transpose(tf.stack(whole[max_sentence_len]))
    label = tf.transpose(tf.stack(whole[max_sentence_len + 1:]))
    return features, length, label
def read_w2v(index_file):
    fp = open(index_file)
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
def init_model(c2v):
    # word2vec variable
    print ("c2v:",c2v)
    words = tf.Variable(c2v, name="words")
    print ("words:",words.get_shape())
    # input placeholder
    inp = tf.placeholder(tf.int32, [None, max_sentence_len], name='input_x')
    # sequence real lenth
    seq_length = tf.placeholder(tf.int32, [None], name='seq_length')
    # choose model
    index_model = choose_model()
    return index_model
def choose_model():
    return RNN([100],max_sentence_len,3,0.5)
def loss(X, Y, seq_length):
    index_score = inference(X, seq_length)
    print ("index_score",index_score.get_shape())
    cross_entropy = tf.nn.softmax_cross_entropy_with_logits(logits=index_score, labels=Y)
    print ("cross_entropy:",cross_entropy.get_shape())
    loss = tf.reduce_mean(cross_entropy)
    return loss
def inference(index_model, X, seq_length,words,trainMode=True):
    word_vectors = tf.nn.embedding_lookup(words, X)
    # print ("word_vectors,",word_vectors.get_shape())
    reuse = False if trainMode else True
    unary_scores = index_model.scores(word_vectors,seq_length, train_model=trainMode, reuse=reuse)
    # if trainMode:
    #     self.rnn_output = rnn_output
    #     self.word_vectors = word_vectors
    return unary_scores
# graph = tf.Graph()
# # tf.reset_default_graph()
# with graph.as_default():
#     X, length, Y = inputs("../data/1342_3298/train_batch_data.txt")
#     c2v,map_w2v = read_w2v("../data/1342_vec.txt")
#     # c2v = load_w2v("../../../ner/train/nvzhuang100/vec.txt",50)
#     words = tf.Variable(c2v, name="words")
#     print ("words x:",words.get_shape(),X.get_shape())
#     Model = init_model()
#     # loss function
#     total_loss = loss(tr_x, tr_y, seq_length_tr)
#     # learn rate and 
#     train_op = train_step(total_loss)
#     # input_ids = tf.placeholder(dtype=tf.int32, shape=[None])
#     # input_embedding = tf.nn.embedding_lookup(words, input_ids)

#     # word_vectors = tf.nn.embedding_lookup(words, X)
#     # sess = tf.Session()
#     # sess.run(tf.global_variables_initializer())
#     sv = tf.train.Supervisor(graph=graph, logdir="")
#     with sv.managed_session(master='') as sess:
#         # print(sess.run(input_embedding, feed_dict={input_ids:[1, 2, 3, 0, 3, 2, 1]}))
#         # word_vectors_i = sess.run(word_vectors)
#         # print (word_vectors_i)
def test_evaluate(self,sess,unary_score,inp,tX, tY,seq_place,seq_length):
    totalEqual = 0
    batchSize = 100
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
        content += ",".join(str(i) for i in predict_res)
        # correct_pred = tf.equal(predict_res, tf.argmax(tY, 1))
        # accuracy = tf.reduce_mean(tf.cast(correct_pred, tf.float32), name='accuracy')

    accuracy = 100.0 * correct_labels / float(total_labels)
    print("Accuracy: %.3f%%" % accuracy)
    return accuracy
def read_val(index_file):
    # get val data
    x,y,_f,seq_length = read_csv(index_file)
    return x,y,seq_length
def read_csv(index_file,write_flag=False):
    data = pd.read_csv(index_file, nrows=None)
    index_sentiments = np.squeeze(data.as_matrix(columns=['Sentiment']))
    index_sentiments = np.eye(3)[index_sentiments]
    samples = data.as_matrix(columns=['SentimentText'])[:, 0]
    sample_lengths = []
    tensors = []
    row_number = 0
    word_count = 1
    write_file = index_path + '/train_batch_data.txt'
    if write_flag:
        content_write = ''
        fs = open(write_file,'w')
    for sample in samples:
        sample_words = sample.decode('utf8').split(' ')
        encoded_sample = []
        ii = -1
        for word in sample_words:  # distinct words in list
            if map_w2v.has_key(word):
            #if word in map_w2v:
                value = map_w2v[word]
            else:
                value = map_w2v['<UNK>']
            encoded_sample += [int(value)]
            ii += 1
            if (ii == max_sentence_len - 1):
                last_word = sample_words[-1]
                if map_w2v.has_key(last_word):
                    last_value = map_w2v[last_word]
                else:
                    last_value = map_w2v['<UNK>']
                encoded_sample[-1] = int(last_value)
                break
        if (ii >= max_sentence_len):
            print("sample over max_seq",sample)
            print("sample over max_seq",ii,max_sentence_len)
            raise Exception('Error: Provided sequence length is not sufficient')
        index_seq_length = len(encoded_sample)
        for ij in range(ii + 1,max_sentence_len):
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
def test_unary_score(index_model,inp,seq_length,words):
    P = inference(index_model,inp,seq_length,words)
    softmax = tf.nn.softmax(P, name='predictions')
    # predict_res = tf.argmax(softmax, 1)
    return softmax
def init():
    words = tf.Variable(c2v, name="words")
    print ("words:",self.words.get_shape())
    # input placeholder
    self.inp = tf.placeholder(tf.int32, [None, self.config.max_seq], name='input_x')
    # sequence real lenth
    self.seq_length = tf.placeholder(tf.int32, [None], name='seq_length')
    # choose model
    index_model = self.choose_model()
    return index_model
graph = tf.Graph()
# tf.reset_default_graph()
with graph.as_default():
    c2v,map_w2v = read_w2v("../data/1342_vec.txt")
    val_x,val_y,seq_length = read_val("../data/1342_3298/val.txt")
    words = tf.Variable(c2v, name="words")
    print ("words:",words.get_shape())
    # input placeholder
    inp = tf.placeholder(tf.int32, [None, max_sentence_len], name='input_x')
    # sequence real lenth
    seq_length_p = tf.placeholder(tf.int32, [None], name='seq_length')
    # choose model
    index_model = choose_model()
    test_unary_score = test_unary_score(index_model,inp,seq_length_p,words)
    sv = tf.train.Supervisor(graph=graph, logdir="")
    with sv.managed_session(master='') as sess:
        sv.saver.restore(sess, '../data/1342_3298/checkpoints/best_model')
        # sv.saver.restore(sess, '/export/mart_risk_aof/wangsanpeng/ner/logs/finnal-model')
        acc = test_evaluate(sess,test_unary_score,val_x,val_y,seq_length_p,seq_length)


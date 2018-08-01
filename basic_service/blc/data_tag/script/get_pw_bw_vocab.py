#coding=utf8
import sys

def write_vocab(outfile, vocab_dict, threshold):
  f = open(outfile, 'w')
  vocab_dict = vocab_dict.items()
  vocab_dict.sort(key=lambda x:x[1], reverse=True)
  for vocab, count in vocab_dict:
    if count < threshold or vocab == '\N':
      continue
    s = '%s\t%s\n' % (vocab, count)
    f.write(s)

def extract_vocab(data_file, brand_vocab_file, product_vocab_file, threshold):
  brand_vocab = dict()
  product_vocab = dict()
  f = open(data_file, 'r')
  for line in f:
    items = line.strip().split('\t')
    if len(items) < 4:
      continue
    skuid, cn_brand, en_brand, products = items[:4]
    cn_brand = cn_brand.strip()
    en_brand = en_brand.strip()
    brand_vocab[cn_brand] = brand_vocab.get(cn_brand, 0) + 1
    brand_vocab[en_brand] = brand_vocab.get(en_brand, 0) + 1
    products = products.split(',')
    for product in products:
      product = product.strip()
      product_vocab[product] = product_vocab.get(product, 0) + 1
  write_vocab(brand_vocab_file, brand_vocab, threshold)
  write_vocab(product_vocab_file, product_vocab, threshold)

if __name__ == '__main__':
  data_file = sys.argv[1]
  brand_vocab_file = sys.argv[2]
  product_vocab_file = sys.argv[3]
  extract_vocab(data_file, brand_vocab_file, product_vocab_file, 2)

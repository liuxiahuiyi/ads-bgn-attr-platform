#coding=utf8

vocab_matcher_infos = [('brand', './vocab/brand_word.dat', 'value', '', 'brand', 2),
                       ('product', './vocab/product_word.dat', 'value', '', 'product', 2),
                       #('word_type', './vocab/word_type.dat', 'value_key', ',', 'wt', 1),
                      ]

g_feature_vocab_list = [('brand', './vocab/brand_word.dat'),
                        ('product', './vocab/product_word.dat'),
                        ('material', './vocab/material.dat'),
                        ('scene', './vocab/scene.dat'),
                        ('style', './vocab/style.dat'),
                       ]

g_care_labels = set([u'材质属性', 
                     u'样式属性', 
                     u'颜色属性',
                     #u'规格属性', 
                     u'型号词', 
                     u'产品词',
                     u'品牌词', 
                     u'场景属性', 
                     u'风格属性',
                     u'人群属性', 
                     u'功能属性', 
                     u'尺码属性',
                     u'季节属性', 
                     u'适用属性',
                    ])

g_eval_labels = set([u'材质属性', 
                     u'样式属性', 
                     u'颜色属性',
                     #u'规格属性', 
                     u'型号词', 
                     u'产品词',
                     u'品牌词', 
                     u'场景属性', 
                     u'风格属性',
                     u'人群属性', 
                     u'功能属性', 
                     u'尺码属性',
                     u'季节属性', 
                     u'适用属性',
                     u'SEG',
                    ])

import numpy as np
import pandas as pd
import keras
from keras.utils import np_utils
import random
from random import randrange
import os
import shutil
import tensorflow as tf
from keras.datasets import mnist
from keras import backend as K
from keras.preprocessing.image import ImageDataGenerator
#import tensorflow_datasets as tfds

class DataSource(object):
    def __init__(self):
        raise NotImplementedError()
    def partitioned_by_rows(self, num_workers, test_reserve=.3):
        raise NotImplementedError()
    def sample_single_non_iid(self, weight=None):
        raise NotImplementedError()

class VggFace2(DataSource):
    data_dir = './vgg_face2'

    def padding(self, array, xx, yy):
        """
        :param array: numpy array
        :param xx: desired height
        :param yy: desirex width
        :return: padded array
        """

        h = array.shape[0]
        w = array.shape[1]
        z = 3

        a = (xx - h) // 2
        aa = xx - a - h

        b = (yy - w) // 2
        bb = yy - b - w
        
        l1 = np.pad(array[:,:,0], pad_width=((a, aa), (b, bb)), mode='constant')
        l2 = np.pad(array[:,:,1], pad_width=((a, aa), (b, bb)), mode='constant')
        l3 = np.pad(array[:,:,2], pad_width=((a, aa), (b, bb)), mode='constant')

        return np.stack([l1, l2, l3],axis=2) 

    def __init__(self):
        # from numpy import genfromtxt
        #lab = randrange(4)+1
        lab = randrange(6)+1
        #print("label randomly chosen")
        #print(lab)
        data = np.loadtxt(f'./Path_with_labels{lab}.csv', dtype='str', delimiter=',')
        data = data[1:-1,:]
        # data = pd.read_csv("./Path_with_labels.csv")

        train_datagen = ImageDataGenerator()

        random.seed(2021)
        random_file = random.choice(os.listdir(self.data_dir))
        #print(random_file)
        
        #tmp = str(random.random())
        #path = os.path.join(self.data_dir, tmp)
        classpath = os.path.join(self.data_dir, random_file)
        path = classpath
        #print()
        #print(classpath)
        img_height = 224
        img_width = 224
        batch_size = 10
        #shutil.copytree(self.data_dir + "/" + random_file, classpath)
        #print("printing old path")
        #print(path)
        path = './vgg_face2/n000838/'
        
        # train_data = tf.keras.utils.image_dataset_from_directory(
        #     path,
        #     validation_split=0.2,
        #     subset="training",
        #     seed=123,
        #     image_size=(img_height, img_width),
        #     batch_size=batch_size)
        train_data = []
        test_data = []
        valid_data = []
        from sklearn.model_selection import train_test_split
        import cv2
        train, test = train_test_split(data, test_size = 0.3, random_state = 1)
        valid, test = train_test_split(test, test_size = 0.5, random_state = 1)

        #print(train)
        #print(type(train[1,0]))
        #print(train[1,0])
        # print(os.listdir(train[1,0]))
        
        for filepath in train[1:,0]:
            #print(f"filepath is {filepath}")
            nparr = cv2.imread(filepath)
            #print(nparr.shape)
            #print(type(nparr))
            # nparr = cv2.cvtColor(nparr, cv2.COLOR_BGR2GRAY)
            if nparr.shape[0] <= 300 and nparr.shape[1] <=300:
                train_data.append(self.padding(nparr,300,300))
            
        for filepath in valid[1:,0]:
            nparr = cv2.imread(filepath)
            # nparr = cv2.cvtColor(nparr, cv2.COLOR_BGR2GRAY)
            if nparr.shape[0] <= 300 and nparr.shape[1] <=300:
                valid_data.append(self.padding(nparr,300,300))

        for filepath in test[1:,0]:
            nparr = cv2.imread(filepath)
            # nparr = cv2.cvtColor(nparr, cv2.COLOR_BGR2GRAY)
            if nparr.shape[0] <= 300 and nparr.shape[1] <=300:
                test_data.append(self.padding(nparr,300,300))

        #print(type(train_data[0]))

        # train_data = train_datagen.flow_from_dataframe(
        #     dataframe = train,
        #     x_col = 'File_Path',
        #     y_col = 'Labels',
        #     class_mode = 'raw',
        #     target_size = (img_height, img_width),
        #     batch_size = batch_size
        # )

        # test_data = train_datagen.flow_from_dataframe(
        #     dataframe = test,
        #     x_col = 'File_Path',
        #     y_col = 'Labels',
        #     class_mode = 'raw',
        #     target_size = (img_height, img_width),
        #     batch_size = batch_size
        # )
        #print(train_data[0].shape)
        #print(test_data[0].shape)
        #print(valid_data[0].shape)

        # test_data = tf.keras.utils.image_dataset_from_directory(
        #     path,
        #     validation_split=0.2,
        #     subset="validation",
        #     seed=123,
        #     image_size=(img_height, img_width),
        #     batch_size=batch_size)
        # print(test_data)
        # TODO: add test_data?
        #print(train_data.shape)
        #print(labels_train.shape)
        #print(test_data.shape)
        #print(labels_test.shape)
        # shutil.rmtree(path)
        self.x_train = train_data
        #self.y_train = labels_train
        self.x_test = test_data
        self.x_valid = valid_data
        #self.y_train = labels_test

    def fake_non_iid_data(self, min_train=100, max_train=1000, data_split=(.6,.3,.1)): 
        #print("checking the data type of x_train")
        #print(self.x_train.shape)
        #print(self.x_test.shape)

        #TODO : fake non iid data
        #my_class_distr = [1. / self.classes.shape[0] * self.classes.shape[0]]      

        #train_size = random.randint(min_train, max_train)
        #test_size = int(train_size / data_split[0] * data_split[1])
        #valid_size = int(train_size / data_split[0] * data_split[2])

        #train_set = [self.sample_single_non_iid(self.x_train, self.y_train, my_class_distr) for _ in range(train_size)]
        #test_set = [self.sample_single_non_iid(self.x_test, self.y_test, my_class_distr) for _ in range(test_size)]
        #valid_set = [self.sample_single_non_iid(self.x_valid, self.y_valid, my_class_distr) for _ in range(valid_size)]
        #print("done generating fake data")

        return ((self.x_train, self.x_test, self.x_valid), [1])
        # return ((np.stack(list(self.x_train)), np.stack(list(self.x_test)), np.stack(list(self.x_test))), [1]) 
        # return ((tfds.as_numpy(self.x_train), tfds.as_numpy(self.x_test), tfds.as_numpy(self.x_test)), [1]) 
    
if __name__ == "__main__":
    m = VggFace2()
    # res = m.partitioned_by_rows(9)
    # print(res["test"][1].shape)
    #for _ in range(10):
        #print(m.gen_dummy_non_iid_weights())


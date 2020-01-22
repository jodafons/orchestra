
import numpy as np

from keras.datasets import mnist
# the data, split between train and test sets
(x_train, y_train), (x_test, y_test) = mnist.load_data()
data = {
        'x_train' : x_train,
        'y_train' : y_train,
        'x_test'  : x_test,
        'y_test'  : y_test,
    }

import pickle
f=open('my_data.pic','wb')
pickle.dump(data,f)
f.close()





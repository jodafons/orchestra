#!/usr/bin/env python

try:
  from tensorflow.compat.v1 import ConfigProto
  from tensorflow.compat.v1 import InteractiveSession
  config = ConfigProto()
  config.gpu_options.allow_growth = True
  session = InteractiveSession(config=config)
except Exception as e:
  print(e)
  print("Not possible to set gpu allow growth")

import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout, Flatten
from tensorflow.keras.layers import Conv2D, MaxPooling2D
from tensorflow.keras import backend as K
import numpy as np
import argparse
import sys,os


parser = argparse.ArgumentParser(description = '', add_help = False)
parser = argparse.ArgumentParser()


parser.add_argument('-c','--configFile', action='store',
        dest='configFile', required = True,
            help = "The job config file that will be used to configure the job (sort and init).")

parser.add_argument('-o','--outputFile', action='store',
        dest='outputFile', required = False, default = None,
            help = "The output tuning name.")

parser.add_argument('-d','--dataFile', action='store',
        dest='dataFile', required = False, default = None,
            help = "The data/target file used to train the model.")


if len(sys.argv)==1:
  parser.print_help()
  sys.exit(1)

args = parser.parse_args()



config = dict(np.load( args.configFile,allow_pickle=True ))
data = dict(np.load( args.dataFile,allow_pickle=True ))


# the training configuration
batch_size = config['batch_size']
num_classes = config['num_classes']
epochs = config['epochs']
sort = config['sort']
init = config['init']

# the data, split between train and test sets
x_train = data['x_train']
y_train = data['y_train']
x_test = data['x_test']
y_test = data['y_test']


# input image dimensions
img_rows, img_cols = 28, 28

if K.image_data_format() == 'channels_first':
    x_train = x_train.reshape(x_train.shape[0], 1, img_rows, img_cols)
    x_test = x_test.reshape(x_test.shape[0], 1, img_rows, img_cols)
    input_shape = (1, img_rows, img_cols)
else:
    x_train = x_train.reshape(x_train.shape[0], img_rows, img_cols, 1)
    x_test = x_test.reshape(x_test.shape[0], img_rows, img_cols, 1)
    input_shape = (img_rows, img_cols, 1)


x_train = x_train.astype('float32')
x_test = x_test.astype('float32')
x_train /= 255
x_test /= 255

print('x_train shape:', x_train.shape)
print(x_train.shape[0], 'train samples')
print(x_test.shape[0], 'test samples')

# convert class vectors to binary class matrices
y_train = keras.utils.to_categorical(y_train, num_classes)
y_test = keras.utils.to_categorical(y_test, num_classes)

model = Sequential()
model.add(Conv2D(32, kernel_size=(3, 3),
                 activation='relu',
                 input_shape=input_shape))
model.add(Conv2D(64, (3, 3), activation='relu'))
model.add(MaxPooling2D(pool_size=(2, 2)))
model.add(Dropout(0.25))
model.add(Flatten())
model.add(Dense(128, activation='relu'))
model.add(Dropout(0.5))
model.add(Dense(num_classes, activation='softmax'))

model.compile(loss='categorical_crossentropy',
              optimizer='adam',
              metrics=['accuracy'])

epochs=1
history = model.fit(x_train, y_train,
          batch_size=batch_size,
          epochs=epochs,
          verbose=1,
          validation_data=(x_test, y_test)).history

score = model.evaluate(x_test, y_test, verbose=0)
print('Test loss:', score[0])
print('Test accuracy:', score[1])


# output file
from tensorflow.keras.models import model_from_json
import json


output = {
      'history' : dict(history),
      'score'   : score,
      'model'   : model.to_json(),
      'sort'    : sort,
      'init'    : init,
    }

from pprint import pprint
pprint(output)

output_name = args.outputFile + "_s%d_i%d.pic" % (sort,init)

import pickle
f=open(output_name,'wb')
pickle.dump(output,f)
f.close()








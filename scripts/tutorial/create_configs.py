
import numpy as np

# my config job example
number_of_sorts = 2
number_of_inits = 2
batch_size = 128
num_classes = 10
epochs = 12


# loop over each cv
for sort in range(number_of_sorts):
  for init in range(number_of_inits):

    config = {
               'sort' : sort,
               'init' : init,
               'batch_size' : batch_size,
               'num_classes': num_classes,
               'epochs'     : epochs,
              }
    fname = 'config_s%d_i%d' % (sort,init)
    np.save( fname, config )







from keras.datasets import mnist
# the data, split between train and test sets
(x_train, y_train), (x_test, y_test) = mnist.load_data()
data = {
        'x_train' : x_train,
        'y_train' : y_train,
        'x_test'  : x_test,
        'y_test'  : y_test,
    }

np.save("data.npz", data)





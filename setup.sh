

rm -rf build
mkdir build
cd build
ln -s ../gaugi/python Gaugi
ln -s ../orchestra/python orchestra
export PYTHONPATH=`pwd`:$PYTHONPATH
export PYTHONPATH=`pwd`:$PYTHONPATH
cd ..
export PATH=`pwd`/scripts:$PATH



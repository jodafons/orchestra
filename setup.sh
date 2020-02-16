

rm -rf __python__
mkdir __python__
cd __python__
ln -s ../Gaugi/python Gaugi
ln -s ../orchestra/python orchestra
export PYTHONPATH=`pwd`:$PYTHONPATH
export PYTHONPATH=`pwd`:$PYTHONPATH
cd ..
export PATH=`pwd`/scripts:$PATH



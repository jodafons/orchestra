


mkdir .python_dir
cd .python_dir

ln -s ../Gaugi/python Gaugi
ln -s ../orchestra/python orchestra
export PYTHONPATH=`pwd`:$PYTHONPATH
cd ../

export PATH=`pwd`/scripts:$PATH



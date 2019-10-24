


#rm -rf .python_dir
mkdir .python_dir
cd .python_dir
mkdir python
cd python
ln -s ../../Gaugi/python Gaugi
ln -s ../../orchestra/python orchestra

export PYTHONPATH=`pwd`:$PYTHONPATH
cd ../../



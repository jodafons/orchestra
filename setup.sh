export LC_ALL=''
export RCM_NO_COLOR=0
export RCM_GRID_ENV=0

rm -rf .__python__
mkdir .__python__
cd .__python__
ln -s ../orchestra/python orchestra
export PYTHONPATH=`pwd`:$PYTHONPATH
cd ..
export PATH=`pwd`/scripts:$PATH



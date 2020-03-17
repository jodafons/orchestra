export LC_ALL=''
export RCM_NO_COLOR=0
export RCM_GRID_ENV=0

if test ! -d "$PWD/.__python__" ; then
  echo "file __python__ not exist"
  mkdir .__python__
fi

cd .__python__
rm orchestra
ln -sf ../orchestra/python orchestra
export PYTHONPATH=`pwd`:$PYTHONPATH
cd ..
export PATH=`pwd`/scripts:$PATH



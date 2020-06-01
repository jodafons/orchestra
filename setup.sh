export LC_ALL=''
export RCM_NO_COLOR=0
export RCM_GRID_ENV=0

echo "Setup orchestra envs..."

if test ! -d "$PWD/.__python__" ; then
  echo "file __python__ not exist"
  mkdir .__python__
  cd .__python__
  ln -sf ../orchestra/python orchestra
  ln -sf ../external/partitura/python partitura
fi


cd .__python__
export PYTHONPATH=`pwd`:$PYTHONPATH
cd ..
export PATH=`pwd`/scripts:$PATH



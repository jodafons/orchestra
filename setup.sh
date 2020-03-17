export LC_ALL=''
export RCM_NO_COLOR=0
export RCM_GRID_ENV=0

if test ! -d "$PWD/.__python__" ; then
  echo "file __python__ not exist"
  mkdir .__python__
  cd .__python__
  ln -sf ../orchestra/python orchestra
fi


cd .__python__
export PYTHONPATH=`pwd`:$PYTHONPATH
cd ..
export PATH=`pwd`/scripts:$PATH


# This will import all LPS credentials and DB configs from the private repository
source external/partitura/scripts/setup_envs.sh


#!/bin/bash

export Athena_SETUP=off
export CPLUS_INCLUDE_PATH="$CPLUS_INCLUDE_PATH:/usr/include/python2.7/"
export LC_ALL=''
export RCM_NO_COLOR=0
export RCM_GRID_ENV=0

export SAPHYRA_PATH=`pwd`

cd build
rm -rf lib
mkdir lib
for file in "`pwd`"/**/*.pcm
do
  echo "ln -sf $file lib"
  ln -sf $file lib
done 

for file in "`pwd`"/*.dylib
do
  echo "ln -sf $file lib"
  ln -sf $file lib
done 

export LD_LIBRARY_PATH=`pwd`/lib:$LD_LIBRARY_PATH
export PYTHONPATH=`pwd`:$LD_LIBRARY_PATH:$PYTHONPATH
export PYTHONPATH=`pwd`/python:$PYTHONPATH
export PATH=`pwd`/scripts:$PATH
cd ..


#!/bin/bash

MY_USER=$USER
current=$PWD

cd $(mktemp -d)
temp=$PWD

git clone https://github.com/ringer-softwares/orchestra.git

echo "=========================================================="
echo "setup root..."
source /setup_root.sh
echo "=========================================================="
echo "setup orchestra..."
cd $temp/orchestra && source setup.sh
echo "=========================================================="

cd $current












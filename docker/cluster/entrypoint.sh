#!/bin/bash

current=$PWD

cd $(mktemp -d)
temp=$PWD
git clone https://github.com/jodafons/ringer.git
git clone https://github.com/jodafons/prometheus.git && cd prometheus && mkdir build && cd build && cmake .. && make && cd ../../
git clone https://github.com/jodafons/orchestra.git



echo "=========================================================="
echo "setup root..."
source /opt/root/buildthis/bin/thisroot.sh
echo "=========================================================="
echo "update all necessary packages..."
pip install --upgrade Gaugi saphyra
echo "=========================================================="
echo "setup prometheus..."
cd $temp/prometheus && source setup.sh
echo "=========================================================="
echo "setup orchestra..."
cd $temp/orchestra && source setup.sh
echo "=========================================================="
echo "setup ringer scripts..."
cd $temp/ringer && git pull && source setup.sh
echo "=========================================================="

cd $current
maestro.py


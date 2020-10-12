#!/bin/bash

echo "=========================================================="
echo "setup root..."
source /opt/root/buildthis/bin/thisroot.sh
echo "=========================================================="
echo "update all necessary packages..."
pip install --upgrade Gaugi saphyra
echo "=========================================================="
echo "setup prometheus..."
current=$PWD
cd /codes/prometheus && git pull && source setup.sh
echo "=========================================================="
echo "setup orchestra..."
cd /codes/orchestra && git pull && source setup.sh

cd $current

maestro.py


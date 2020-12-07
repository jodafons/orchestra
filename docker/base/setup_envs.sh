#!/bin/bash

current=$PWD

cd $(mktemp -d)
temp=$PWD


git clone https://github.com/jodafons/saphyra.git
git clone https://github.com/jodafons/Gaugi.git
git clone https://github.com/jodafons/ringer_tunings.git
git clone https://github.com/jodafons/orchestra.git

echo "=========================================================="
echo "update all necessary packages..."
cd $temp/Gaugi && source scripts/setup.sh
cd $temp/saphyra && source scripts/setup.sh
echo "=========================================================="
echo "setup orchestra..."
cd $temp/orchestra && source setup.sh
echo "=========================================================="
echo "setup ringer scripts..."
cd $temp/ringer_tunings && git pull && source setup.sh
echo "=========================================================="

cd $current


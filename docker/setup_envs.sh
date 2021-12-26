#!/bin/bash

MY_USER=jodafons
current=$PWD

cd $(mktemp -d)
temp=$PWD

git clone https://github.com/ringer-softwares/gaugi.git
git clone https://github.com/ringer-softwares/rootplotlib.git
git clone https://github.com/ringer-softwares/kepler.git
git clone https://github.com/ringer-softwares/kolmov.git
git clone https://github.com/ringer-softwares/pybeamer.git
git clone https://github.com/ringer-softwares/saphyra.git
git clone https://github.com/ringer-softwares/orchestra.git

echo "=========================================================="
echo "update all necessary packages..."
source /setup_root.sh
cd $temp/gaugi && source scripts/setup.sh
cd $temp/saphyra && source scripts/setup.sh
cd $temp/pybeamer && source scripts/setup.sh

echo "=========================================================="
echo "setup orchestra..."
cd $temp/orchestra && source setup.sh
echo "=========================================================="
echo "setup ringer scripts... "
echo $MY_USER
git clone https://github.com/ringer-softwares/$MY_USER.git


cd $temp/$MY_USER && git pull && source scripts/setup.sh
echo "=========================================================="

cd $current












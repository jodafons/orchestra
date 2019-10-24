


FILE=`pwd`/.python_dir

if [ -f "$FILE" ]; then
  mkdir $FILE
  mkdir $FILE/python
  echo "$FILE does not exist"
else
  echo "$FILE exist"
fi

cd $FILE/python
ln -s ../../Gaugi/python Gaugi
ln -s ../../orchestra/python orchestra
export PYTHONPATH=`pwd`:$PYTHONPATH
cd ../../

export PATH=`pwd`/scripts:$PATH



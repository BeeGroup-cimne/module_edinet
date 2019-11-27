#! /bin/bash

rm tasks.py
c_pwd=`pwd`
base=`basename $c_pwd`
for m in `ls -d */ |grep ^[^_]`
do
  cd $m
  . install.sh
  cd ..

 echo "from $base.${m%?}.launcher import $task_name" >> tasks.py
done

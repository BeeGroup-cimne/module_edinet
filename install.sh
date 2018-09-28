#! /bin/bash

rm tasks.py

for m in `ls -d */ |grep ^[^_]`
do
  cd $m
  . install.sh
  cd ..

 echo "from ${m%?}.launcher import $task_name" >> tasks.py
done

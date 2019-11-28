#! /bin/bash

rm tasks.py
c_pwd=`pwd`
base=`basename $c_pwd`
for m in `ls -d */ |grep ^[^_]`
do
  cd $m
  if [ -f "module_variables.sh" ]; then
  	. install.sh
  	cd ..
 	echo "from $base.${m%?}.launcher import $task_name" >> tasks.py
  else
	cd ..
  fi
done

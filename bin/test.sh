#!/bin/sh
a=1
while [ $a -le 10 ]
do
    java GoogleFileManager &
    a=`expr $a + 1`
done

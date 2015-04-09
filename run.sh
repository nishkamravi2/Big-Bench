querylist="6 7 9 11 12 13 14 15 16 17 21 22 23 24"
for i in $querylist 
do
  /var/lib/jenkins/Big-Bench/bin/bigBench runQuery -q $i
done

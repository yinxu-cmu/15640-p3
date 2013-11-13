# java dfs.YZFS -c lemonshark.ics.cs.cmu.edu &
java dfs.YZFS -c 128.2.247.7 & 
sleep 1
java mapreduce.MapReduceSlave start &
sleep 1
java dfs.YZFS -yzfs copyFromLocal ./inputs
sleep 1
java mapreduce.YZHadoop WordCount


java dfs.YZFS -c 128.2.251.139 & 
sleep 1
java mapreduce.MapReduceSlave start &
sleep 1
java dfs.YZFS -yzfs copyFromLocal ./inputs
sleep 1
java mapreduce.YZHadoop WordCount


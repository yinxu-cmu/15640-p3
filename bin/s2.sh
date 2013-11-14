java dfs.YZFS -c 128.2.251.196 & 
sleep 1
java mapreduce.MapReduceSlave &
sleep 1
java dfs.YZFS -yzfs copyFromLocal ./inputs
sleep 1
# java mapreduce.YZHadoop WordCount
java mapreduce.YZHadoop Maximum

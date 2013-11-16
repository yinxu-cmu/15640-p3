java dfs.YZFS -c 128.2.247.123 & 
sleep 1
java mapreduce.MapReduceSlave &
sleep 1
java dfs.YZFS -yzfs copyFromLocal ./inputs/file1.txt
sleep 1
java mapreduce.YZHadoop Maximum

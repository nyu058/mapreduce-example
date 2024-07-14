hdfs dfs -rm -r linecount_out
hdfs dfs -mkdir -p linecount
hdfs dfs -copyFromLocal shakespeare-1.txt linecount
hadoop jar A1LineCount.jar linecount linecount_out
hdfs dfs -copyToLocal linecount_out/part-00000 ./linecount_output.txt
cat linecount_output.txt
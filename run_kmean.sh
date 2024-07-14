hdfs dfs -rm -r kmean_out
hdfs dfs -mkdir -p kmean
hdfs dfs -copyFromLocal data_points-1.txt kmean
# Args: input output k
hadoop jar A1Kmeans.jar kmean kmean_out 3
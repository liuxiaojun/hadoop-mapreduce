# 上传两个测试的文件到hadoop集群
hadoop fs -mkdir /user/liuxj/avg
hadoop fs -put data1.txt /user/liuxj/avg
hadoop fs -put data2.txt /user/liuxj/avg

# 程序逻辑


# run
yarn jar avg-1.0-SNAPSHOT.jar /user/liuxj/avg /user/liuxj/avg/result

# example
hadoop fs -cat /user/liuxj/avg/result/part*

# 上传两个测试的文件到hadoop集群
hadoop fs -mkdir /user/liuxj/avg

hadoop fs -put eightteen_a.txt /user/liuxj/avg

hadoop fs -put eightteen_b.txt /user/liuxj/avg


# run
yarn jar MaxMinValue-1.0-SNAPSHOT.jar /user/liuxj/maxmin /user/liuxj/maxmin/result

# result
hadoop fs -cat /user/liuxj/maxmin/result/part*

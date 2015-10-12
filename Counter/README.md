# 创建测试文件 data.txt,内容如下
0
1
2
3
4
11
22
33
44
55
111
222
333
444
555
666

# 将此文件传到集群上面
hadoop fs -put data.txt /user/liuxj/data

# 运行
yarn jar Counter-1.0-SNAPSHOT.jar /user/liuxj/data/* /user/liuxj/data/res


# part of result
  ErrorCounter
        toolong=22
        tooshort=5

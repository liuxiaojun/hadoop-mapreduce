# code
distinctData/

# run
[liuxj@1.master.adh ~]$ hadoop fs -cat /user/liuxj/test/file1.txt
2012-3-1 a
2012-3-2 b
2012-3-3 c
2012-3-4 d
2012-3-5 a
2012-3-6 b
2012-3-7 c
2012-3-8 c

[liuxj@1.master.adh ~]$ hadoop fs -cat /user/liuxj/test/file2.txt
2012-3-1 b
2012-3-2 a
2012-3-3 b
2012-3-4 d
2012-3-5 a
2012-3-6 c
2012-3-7 d
2012-3-3 c


# example
yarn jar distinctData-1.0-SNAPSHOT.jar /user/liuxj/test /user/liuxj/test/output

#result
[liuxj@1.master.adh ~]$ hadoop fs -cat /user/liuxj/test/output/part-r-00000
2012-3-1 a
2012-3-1 b
2012-3-2 a
2012-3-2 b
2012-3-3 b
2012-3-3 c
2012-3-4 d
2012-3-5 a
2012-3-6 b
2012-3-6 c
2012-3-7 c
2012-3-7 d
2012-3-8 c

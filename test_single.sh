#!/bin/bash

# 初始化计数器
pass_count=0
fail_count=0

# 循环执行5次
for ((i=1; i<=5; i++)); 
do 
    echo "ROUND $i"
    rm -rf /tmp/*test-raftstore*
    GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestConfChangeRecover3B > ./out/out-$i.txt
    rm -rf /tmp/*test-raftstore*
    # 检查 make 命令是否成功
    if grep -q "FAIL" ./out/out-$i.txt; then
        ((fail_count++))  # 如果输出中有 "FAIL"，增加失败计数
        echo "FAIL"
    else
        ((pass_count++))  # 如果输出中没有 "FAIL"，增加通过计数
        echo "PASS"
    fi
done

# 输出通过和失败次数
echo "Total Passed: $pass_count"
echo "Total Failed: $fail_count"

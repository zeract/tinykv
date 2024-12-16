#!/bin/bash

# 初始化计数器
pass_count=0
fail_count=0

# 创建输出目录
mkdir -p ./out

# 循环执行50次
for ((i=1; i<=5; i++)); 
do 
    echo "ROUND $i"
    
    # 执行 make project3b，并将输出保存到文件
    LOG_LEVEL=fatal make project3b > ./out/out-$i.txt
    
    # 检查输出文件中是否包含 "FAIL"
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

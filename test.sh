#!/bin/bash

# 初始化计数器
pass_count=0
fail_count=0

# 循环执行50次
for ((i=1; i<=50; i++)); 
do 
    echo "ROUND $i"
    
    # 执行 make project2c，并将输出保存到文件
    make project3a > ./out/out-$i.txt
    
    # 检查 make 命令是否成功
    if [ $? -eq 0 ]; then
        ((pass_count++))  # 如果成功，增加通过计数
    else
        ((fail_count++))  # 如果失败，增加失败计数
    fi
done

# 输出通过和失败次数
echo "Total Passed: $pass_count"
echo "Total Failed: $fail_count"


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
脚本用于提取final_dataset.json文件中所有包含golden_sql的数据
"""

import json
import os
from typing import List, Dict, Any


def extract_golden_sql_data(dataset_file_path: str, output_file_path: str) -> None:
    """
    从final_dataset.json文件中提取所有包含golden_sql的数据
    
    Args:
        dataset_file_path: final_dataset.json文件路径
        output_file_path: 输出的JSON文件路径
    """
    try:
        # 读取final_dataset.json文件
        with open(dataset_file_path, 'r', encoding='utf-8') as file:
            dataset_data = json.load(file)
        
        # 提取包含golden_sql的数据
        golden_sql_records = []
        
        for record in dataset_data:
            # 检查记录中是否包含golden_sql字段
            if 'golden_sql' in record and record['golden_sql']:
                golden_sql_records.append(record)
        
        # 将提取的信息写入输出文件
        with open(output_file_path, 'w', encoding='utf-8') as output_file:
            json.dump(golden_sql_records, output_file, ensure_ascii=False, indent=2)
        
        print(f"成功提取 {len(golden_sql_records)} 个包含golden_sql的记录")
        
        # 显示提取记录的sql_id
        if golden_sql_records:
            print(f"\n提取的记录ID:")
            for i, record in enumerate(golden_sql_records, 1):
                sql_id = record.get('sql_id', 'N/A')
                question = record.get('question', 'N/A')
                golden_sql = record.get('golden_sql', 'N/A')
                print(f"  {i}. {sql_id}")
                print(f"     问题: {question[:100]}{'...' if len(str(question)) > 100 else ''}")
                if golden_sql and not isinstance(golden_sql, bool):
                    golden_sql_str = str(golden_sql)
                    print(f"     golden_sql: {golden_sql_str[:100]}{'...' if len(golden_sql_str) > 100 else ''}")
                print()
        
        print(f"结果已保存到: {output_file_path}")
        
    except FileNotFoundError:
        print(f"错误: 找不到文件 {dataset_file_path}")
    except json.JSONDecodeError:
        print(f"错误: {dataset_file_path} 不是有效的JSON文件")
    except Exception as e:
        print(f"发生错误: {str(e)}")


def main():
    """主函数"""
    # 设置文件路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_file_path = os.path.join(current_dir, '../data/final_dataset.json')
    output_file_path = os.path.join(current_dir, '../data/golden_sql.json')
    
    # 执行提取
    extract_golden_sql_data(dataset_file_path, output_file_path)


if __name__ == "__main__":
    main()
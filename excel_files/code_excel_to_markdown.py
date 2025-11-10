#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Excel文件转Markdown表格工具
将多个Excel文件转换为Markdown格式的表格，并输出到单个MD文件中
"""

import os
import pandas as pd
from pathlib import Path
import sys
from typing import List, Dict
import argparse


def find_excel_files(directory: str) -> List[str]:
    """查找指定目录下的所有Excel文件"""
    excel_extensions = ['.xlsx', '.xls', '.xlsm', '.xlsb']
    excel_files = []
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if any(file.lower().endswith(ext) for ext in excel_extensions):
                excel_files.append(os.path.join(root, file))
    
    return excel_files


def read_excel_file(file_path: str) -> Dict[str, pd.DataFrame]:
    """读取Excel文件，返回包含所有工作表的字典"""
    try:
        # 读取Excel文件的所有工作表
        excel_file = pd.ExcelFile(file_path)
        sheets_data = {}
        
        for sheet_name in excel_file.sheet_names:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            # 处理NaN值
            df = df.fillna('')
            sheets_data[sheet_name] = df
        
        return sheets_data
    except Exception as e:
        print(f"错误：无法读取文件 {file_path}: {str(e)}")
        return {}


def dataframe_to_markdown(df: pd.DataFrame, table_name: str) -> str:
    """将DataFrame转换为Markdown表格格式"""
    if df.empty:
        return f"## {table_name}\n\n*表格为空*\n\n"
    
    # 重置索引，避免索引被当作数据列
    df_copy = df.copy()
    df_copy.index = range(1, len(df_copy) + 1)
    
    # 转换为字符串，处理特殊字符
    markdown_lines = []
    
    # 表头
    columns = list(df_copy.columns)
    markdown_lines.append("| " + " | ".join(str(col) for col in columns) + " |")
    
    # 分隔线
    markdown_lines.append("| " + " | ".join("---" for _ in columns) + " |")
    
    # 数据行
    for _, row in df_copy.iterrows():
        row_data = []
        for value in row:
            # 处理特殊字符，替换为Markdown安全格式
            value_str = str(value)
            value_str = value_str.replace('|', '\\|')  # 转义管道符
            value_str = value_str.replace('\n', '<br>')  # 替换换行符
            value_str = value_str.replace('\r', '')  # 移除回车符
            row_data.append(value_str)
        markdown_lines.append("| " + " | ".join(row_data) + " |")
    
    return f"## {table_name}\n\n" + "\n".join(markdown_lines) + "\n\n"


def convert_excel_to_markdown(excel_files: List[str], output_file: str, directory: str = None):
    """将Excel文件转换为Markdown并保存到文件"""
    markdown_content = []
    
    # 添加标题和说明
    markdown_content.append("# Excel文件转换结果\n")
    markdown_content.append(f"转换时间：{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    markdown_content.append(f"转换文件数量：{len(excel_files)}\n\n")
    
    for excel_file in excel_files:
        file_path = excel_file
        file_name = Path(excel_file).stem
        relative_path = os.path.relpath(excel_file, directory) if directory else os.path.basename(excel_file)
        
        print(f"正在处理：{relative_path}")
        
        # 读取Excel文件
        sheets_data = read_excel_file(file_path)
        
        if not sheets_data:
            markdown_content.append(f"## {file_name}\n\n*无法读取文件或文件为空*\n\n")
            continue
        
        # 为每个工作表创建Markdown表格
        for sheet_name, df in sheets_data.items():
            # 确定表名
            if len(sheets_data) == 1:
                table_name = f"📊 {file_name}"
            else:
                table_name = f"📊 {file_name} - {sheet_name}"
            
            # 转换为Markdown
            markdown_table = dataframe_to_markdown(df, table_name)
            markdown_content.append(markdown_table)
        
        # 添加文件信息
        markdown_content.append(f"*数据来源：{relative_path}*\n\n")
        markdown_content.append("---\n\n")
    
    # 写入文件
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.writelines(markdown_content)
        print(f"\n✅ 转换完成！输出文件：{output_file}")
        print(f"📊 共处理 {len(excel_files)} 个Excel文件")
        print(f"📝 包含 {len([line for line in markdown_content if line.startswith('##')])} 个表格")
        
    except Exception as e:
        print(f"❌ 保存文件时出错：{str(e)}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Excel文件转Markdown表格工具')
    parser.add_argument('directory', nargs='?', default='/Users/rdn/Documents/study/tencent/demo/excel_files',
                       help='包含Excel文件的目录路径')
    parser.add_argument('-o', '--output', default='excel_conversion_result.md',
                       help='输出Markdown文件名 (默认：excel_conversion_result.md)')
    
    args = parser.parse_args()
    
    # 检查目录是否存在
    if not os.path.exists(args.directory):
        print(f"❌ 错误：目录 '{args.directory}' 不存在")
        sys.exit(1)
    
    # 查找Excel文件
    excel_files = find_excel_files(args.directory)
    
    if not excel_files:
        print(f"❌ 在目录 '{args.directory}' 中未找到Excel文件")
        print("支持的格式：.xlsx, .xls, .xlsm, .xlsb")
        sys.exit(1)
    
    print(f"🔍 在目录 '{args.directory}' 中找到 {len(excel_files)} 个Excel文件：")
    for file in excel_files:
        relative_path = os.path.relpath(file, args.directory)
        print(f"  - {relative_path}")
    print()
    
    # 转换文件
    convert_excel_to_markdown(excel_files, args.output, args.directory)


if __name__ == "__main__":
    main()
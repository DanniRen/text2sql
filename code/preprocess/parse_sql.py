import json
import re
import nltk
import jieba  # 导入 jieba 用于中文分词
from typing import Dict, List, Any

import process_sql

SQLParser = process_sql.SQLParser

# 确保 nltk 的分词器已下载
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    print("Downloading NLTK 'punkt' tokenizer...")
    nltk.download('punkt')


def tokenize_sql_and_question(query: str, question: str) -> Dict[str, Any]:
    """
    将 SQL 查询和自然语言问题（支持中英文）转换为 Spider 数据集的词元化格式。

    Args:
        query (str): SQL 查询字符串。
        question (str): 自然语言问题字符串（中文或英文）。

    Returns:
        Dict[str, Any]: 包含词元化结果的字典。
    """
    # 1. 词元化 SQL 查询
    query_toks = _tokenize_sql(query)
    
    # 2. 生成 query_toks_no_value (替换具体值为占位符)
    query_toks_no_value = _generate_no_value_tokens(query_toks)

    # 3. 词元化自然语言问题 (支持中英文)
    question_toks = _tokenize_question(question)

    # 4. 组装最终结果
    return {
        "query_toks": query_toks,
        "query_toks_no_value": query_toks_no_value,
        "question": question,
        "question_toks": question_toks,
    }


def _tokenize_question(question: str) -> List[str]:
    """
    辅助函数，用于词元化问题，自动判断中英文并使用相应分词器。
    """
    # 使用正则表达式检测是否包含中文字符
    if re.search(r'[\u4e00-\u9fa5]', question):
        # 如果包含中文，使用 jieba 进行分词
        # jieba.lcut 直接返回列表
        return jieba.lcut(question)
    else:
        # 如果不包含中文，使用 nltk 进行分词
        return nltk.word_tokenize(question)


def _generate_no_value_tokens(tokens: List[str]) -> List[str]:
    """
    生成 Spider 数据集格式的 query_toks_no_value，将具体值替换为占位符。
    """
    no_value_tokens = []
    
    for token in tokens:
        token_lower = token.lower()
        
        # 检查是否为字符串字面量
        if (token.startswith("'") and token.endswith("'")) or \
           (token.startswith('"') and token.endswith('"')):
            no_value_tokens.append("value")
        # 检查是否为数字
        elif token.replace('.', '').replace('-', '').isdigit():
            no_value_tokens.append("value")
        # 检查是否为日期格式
        elif re.match(r'^\d{4}-\d{1,2}-\d{1,2}$', token) or \
             re.match(r'^\d{8}$', token) or \
             re.match(r'^\d{6}$', token):
            no_value_tokens.append("value")
        # 检查是否为日期时间相关函数中的参数
        elif token in ["days", "day", "month", "year", "hour", "minute", "second"]:
            no_value_tokens.append(token_lower)
        # 其他情况保留原token（小写）
        else:
            no_value_tokens.append(token_lower)
    
    return no_value_tokens


def _tokenize_sql(sql: str) -> List[str]:
    """
    辅助函数，专门用于词元化 SQL 查询字符串。
    """
    # 步骤 1: 提取并保护字符串字面量
    string_literals = {}
    placeholder_counter = 0
    pattern = r"('(?:\\'|[^'])*'|\"(?:\\\"|[^\"])*\")"

    def replace_string_with_placeholder(match):
        nonlocal placeholder_counter
        placeholder = f"__STRING_LITERAL_{placeholder_counter}__"
        string_literals[placeholder] = match.group(0)
        placeholder_counter += 1
        return placeholder

    sql_with_placeholders = re.sub(pattern, replace_string_with_placeholder, sql)

    # 步骤 2: 使用正则表达式拆分 SQL 字符串
    split_pattern = r"(\w+|\.\w+|\S)"
    tokens = re.findall(split_pattern, sql_with_placeholders)

    # 步骤 3: 清理和组合词元
    processed_tokens = []
    i = 0
    while i < len(tokens):
        token = tokens[i]

        if i < len(tokens) - 1:
            next_token = tokens[i + 1]
            if token + next_token in ['>=', '<=', '!=', '<>']:
                processed_tokens.append(token + next_token)
                i += 2
                continue

        if token in [' ', '\t', '\n']:
            i += 1
            continue

        processed_tokens.append(token)
        i += 1

    # 步骤 4: 将占位符替换回原始的字符串字面量
    final_tokens = [
        string_literals.get(tok, tok) for tok in processed_tokens
    ]

    return final_tokens


if __name__ == '__main__':
    # 读取文件内容
    with open('../../data/golden_sql.json', 'r', encoding='utf-8') as f:
        data = json.load(f)

    db_id = "text2sql"
    
    # 初始化解析器
    parser = SQLParser()
    
    # 构建 dev.json 格式的数据
    dev_data = []
    
    for item in data:
        if 'sql' in item and 'question' in item:
            sql = item['sql']
            question = item['question']
            
            # 使用 tokenize_sql_and_question 函数处理 SQL 和问题
            tokenized_result = tokenize_sql_and_question(sql, question)
            
            # 使用 SQLParser 解析 SQL 结构
            try:
                sql_label = parser.parse_sql(sql)
            except Exception as e:
                print(f"Warning: Failed to parse SQL: {e}")
                sql_label = None
            
            # 构建 dev.json 格式的条目
            dev_entry = {
                "query": sql,
                "question": question,
                "query_toks": tokenized_result["query_toks"],
                "query_toks_no_value": tokenized_result["query_toks_no_value"],
                "question_toks": tokenized_result["question_toks"],
                "sql": sql_label,  # 添加 SQLParser 解析的嵌套数组结构
                "db_id": db_id
            }
            
            dev_data.append(dev_entry)
    
    # 保存为 dev.json 文件
    output_path = '../../data/dev.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(dev_data, f, ensure_ascii=False, indent=2)
    
    print(f"Successfully generated dev.json with {len(dev_data)} entries at {output_path}")
    
    # 可选：打印第一个条目作为示例
    if dev_data:
        print("\nExample entry:")
        print(json.dumps(dev_data[0], ensure_ascii=False, indent=2))




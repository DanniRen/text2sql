import json
import sys

def convert_json_to_jsonl(input_path, output_path):
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("输入的 JSON 文件必须是一个数组（list），每个元素代表一个表结构。")

    with open(output_path, 'w', encoding='utf-8') as f_out:
        for table in data:
            f_out.write(json.dumps(table, ensure_ascii=False) + '\n')

    print(f"转换完成！已写入 {len(data)} 个表到 {output_path}")

if __name__ == "__main__":
    # 默认输入输出文件名，可自行修改
    input_file = "../../data/schema.json"
    output_file = "../../data/schema.jsonl"

    # 也可以通过命令行参数传入：python script.py input.json schema.jsonl
    if len(sys.argv) == 3:
        input_file = sys.argv[1]
        output_file = sys.argv[2]

    convert_json_to_jsonl(input_file, output_file)
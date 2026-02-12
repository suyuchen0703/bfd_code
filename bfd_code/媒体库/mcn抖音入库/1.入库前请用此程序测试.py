import json

num = 0  # 计数从 0 开始

with open("dy_json.txt", "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            print("JSON解析错误:", line)
            continue

        data = item.get("data")
        # 只处理 data 是 dict 的
        if isinstance(data, dict):
            num += 1
            print(data)

print(f'一共打印了 {num} 条 MCN 数据')

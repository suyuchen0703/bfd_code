import pandas as pd

# === 1. 配置部分 ===
input_file = "入库账号.xlsx"
output_file = "insert_account.sql"
table_name = "mf_media_account_v3"

# === 2. 读取Excel数据 ===
df = pd.read_excel(input_file, dtype=str).fillna("")

# === 3. 生成SQL插入语句 ===
sql_lines = []

# 收集所有非空行的数据
all_values = []
all_columns = set()

# 首先收集所有可能的列和值
for _, row in df.iterrows():
    non_empty_fields = {col: val for col, val in row.items() if val != ""}
    if not non_empty_fields:
        continue
    all_columns.update(non_empty_fields.keys())
    all_values.append(non_empty_fields)

if all_values:
    # 统一所有记录的列（确保每条记录都有相同的列）
    all_columns_list = sorted(list(all_columns))

    # 为每条记录构建值元组
    value_tuples = []
    for record in all_values:
        # 为每条记录构建完整的值列表，缺失的字段用NULL填充
        values_list = []
        for col in all_columns_list:
            val = record.get(col, "")
            if val == "":
                values_list.append("NULL")
            else:
                # 修复f-string中不能包含反斜杠的问题
                escaped_val = str(val).replace("'", "\\'")
                values_list.append(f"'{escaped_val}'")
        value_tuples.append(f"({', '.join(values_list)})")

    # 生成单条INSERT语句插入多条记录
    columns_str = ", ".join([f"`{col}`" for col in all_columns_list])
    values_str = ",\n    ".join(value_tuples)
    sql_statement = f"INSERT INTO {table_name} ({columns_str}) VALUES\n    {values_str};"
    sql_lines.append(sql_statement)

# === 4. 写入文件 ===
with open(output_file, "w", encoding="utf-8") as f:
    f.write("\n".join(sql_lines))

print(f"✅ 已生成 1 条插入语句（包含 {len(all_values)} 条记录），文件已保存到：{output_file}")

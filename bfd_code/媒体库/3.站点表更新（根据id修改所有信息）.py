import pandas as pd

def read_excel_and_generate_update_sql(excel_path, table_name):  # 修正参数名
    """
    读取Excel文件并生成UPDATE SQL语句

    Args:
        excel_path (str): Excel文件路径
        table_name (str): 数据库表名

    Returns:
        str: 生成的SQL语句
    """
    # 读取Excel文件
    df = pd.read_excel(excel_path)

    # 生成SQL更新语句
    sql_statements = []

    for _, row in df.iterrows():
        # 获取ID值
        id_value = row['id']

        # 构建SET子句（排除id字段）
        set_clauses = []
        for column in df.columns:
            if column != 'id':
                value = row[column]
                # 处理不同类型的值
                if pd.isna(value):
                    set_clauses.append(f"{column} = NULL")
                elif isinstance(value, str):
                    # 字符串值需要转义单引号
                    escaped_value = value.replace("'", "''")
                    set_clauses.append(f"{column} = '{escaped_value}'")
                else:
                    # 特殊处理时间字段和其他需要加引号的字段
                    if column in ['created_at', 'updated_at']:
                        set_clauses.append(f"{column} = '{value}'")
                    else:
                        # 数值类型处理，避免添加不必要的小数点
                        if isinstance(value, float) and value.is_integer():
                            # 如果是整数形式的浮点数，转换为整数
                            set_clauses.append(f"{column} = {int(value)}")
                        else:
                            # 其他数值类型直接使用
                            set_clauses.append(f"{column} = {value}")

        # 构建完整的UPDATE语句
        set_clause = ", ".join(set_clauses)
        sql_statement = f"UPDATE {table_name} SET {set_clause} WHERE id = {id_value};"
        sql_statements.append(sql_statement)

    return "\n".join(sql_statements)

# 主程序执行
if __name__ == "__main__":
    # Excel文件路径
    excel_file_path = "站点表更新数据.xlsx"
    # 数据库表名
    db_table_name = "mf_media_site_v3"

    try:
        # 生成SQL语句
        update_sql = read_excel_and_generate_update_sql(excel_file_path, db_table_name)

        # 写入SQL文件
        output_file = "update_statements.sql"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(update_sql)

        print(f"成功生成SQL文件: {output_file}")

    except Exception as e:
        print(f"处理出错: {e}")

import pandas as pd

def generate_bulk_update_sql_from_excel(excel_file_path, output_sql_path):
    """
    从Excel文件读取数据并生成批量更新SQL语句

    Args:
        excel_file_path (str): Excel文件路径
        output_sql_path (str): 输出SQL文件路径
    """

    # 读取Excel文件
    df = pd.read_excel(excel_file_path)

    # 打开SQL文件准备写入
    with open(output_sql_path, 'w', encoding='utf-8') as sql_file:

        # 开始构建CASE语句
        case_when_statements = []
        where_conditions = []

        # 遍历每一行数据构建CASE语句
        for index, row in df.iterrows():
            '''
            在这修改你要通过什么修改什么。。。
            一般id是不变的，修改下面的就行
            列名一定不要写错
            '''
            site_id = row['id']
            new_region = row['website_register_ip_region']

            # 处理空值情况
            if pd.isna(new_region):
                region_value = "NULL"
            else:
                # 转换为字符串并添加引号，正确处理单引号转义
                region_value = f"'{str(new_region).replace(chr(39), chr(39)+chr(39))}'"

            # 添加到CASE语句和WHERE条件中
            case_when_statements.append(f"        WHEN id = {site_id} THEN {region_value}")
            where_conditions.append(str(site_id))

        # 构建完整的UPDATE语句
        if case_when_statements:
            update_sql = "UPDATE mf_media_site_v3 \n"
            '''
            这里的website_register_ip_region别忘了也要改哦。。。
            '''
            update_sql += "SET website_register_ip_region = CASE \n"
            update_sql += "\n".join(case_when_statements) + "\n"
            update_sql += "    END\n"
            update_sql += f"WHERE id IN ({', '.join(where_conditions)});\n"

            # 写入SQL文件
            sql_file.write(update_sql)

    print(f"批量SQL文件已成功生成: {output_sql_path}")

# 使用示例
if __name__ == "__main__":
    # 设置文件路径
    '''
    在这修改文件名哦。。。
    '''
    excel_path = "平台_updated.xlsx"
    sql_output_path = "bulk_update_site_ip.sql"

    # 生成批量SQL文件
    generate_bulk_update_sql_from_excel(excel_path, sql_output_path)

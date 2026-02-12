import json
import pymysql
import redis
from tqdm import tqdm  # pip install tqdm

# ====== 配置区 ======
db_config = {
    "host": "172.18.9.71",
    "port": 3306,
    "user": "bfd_ibg_mf",
    "password": "bfd_ibg_mf@168",
    "database": "bfd_mf_data_v4",
    "charset": "utf8mb4"
}

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_KEY = "dy_mcn_name_fullname_set"

INPUT_FILE = "dy_json.txt"  # 包含多条 JSON 的文本文件
BATCH_SIZE = 100  # 每批插入条数

# ====== 连接数据库 ======
conn = pymysql.connect(**db_config)
cursor = conn.cursor()

# ====== 连接 Redis ======
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

# ====== 初始化 Redis 去重集合 ======
cursor.execute("SELECT name, alias_name FROM mf_media_info_v3 WHERE source_type='huitun'")
for row in cursor.fetchall():
    name, alias_name = row
    # 只要 alias_name 不为空就可以用作 key
    if alias_name is not None:
        key = f"{name or ''}||{alias_name}"
        r.sadd(REDIS_KEY, key)
print("Redis 去重集合初始化数量:", r.scard(REDIS_KEY))

# ====== 插入 SQL（忽略重复） ======
insert_sql = """
INSERT IGNORE INTO mf_media_info_v3 (
    name, alias_name, mcn_desc, mcn_fansCount, mcn_anchorNum, mcn_location, mcn_avatar, media_type, source_type
) VALUES (%s, %s, %s, %s, %s, %s, %s, 2, 'huitun')
"""

# ====== 解析每行 JSON ======
def parse_mcn_line(line):
    try:
        item = json.loads(line)
    except json.JSONDecodeError:
        print("JSON解析错误:", line)
        return None

    data = item.get("data")
    if not isinstance(data, dict):
        return None

    short_name = data.get("shortName")  # name
    full_name = data.get("fullName")    # alias_name

    key = f"{short_name or ''}||{full_name or ''}"
    if r.sismember(REDIS_KEY, key):
        return None  # 已存在，跳过

    # 只要 alias_name（full_name）存在就放入 Redis
    if full_name is not None:
        r.sadd(REDIS_KEY, key)

    return (
        short_name,
        full_name,
        data.get("text"),
        data.get("followers"),
        data.get("users"),
        data.get("location"),
        data.get("avatarUrl")
    )

# ====== 读取文件并准备批量插入 ======
mcn_values = []

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        parsed = parse_mcn_line(line)
        if parsed:
            mcn_values.append(parsed)

# ====== 批量插入数据库 ======
insert_count = 0
total_batches = (len(mcn_values) + BATCH_SIZE - 1) // BATCH_SIZE

for i in tqdm(range(total_batches), desc="批量插入MCN数据"):
    batch = mcn_values[i*BATCH_SIZE : (i+1)*BATCH_SIZE]
    try:
        cursor.executemany(insert_sql, batch)
        insert_count += len(batch)
    except Exception as e:
        print(f"第{i+1}批插入异常:", e)

conn.commit()
cursor.close()
conn.close()

print(f"总共插入 {insert_count} 条 MCN 数据（已通过 name+fullName 联合去重，并存到 Redis）")

import json
import pymysql
from tqdm import tqdm
import redis

# ================= 数据库配置 =================
db_config = {
    "host": "172.18.9.71",
    "port": 3306,
    "user": "bfd_ibg_mf",
    "password": "bfd_ibg_mf@168",
    "database": "bfd_mf_data_v4",
    "charset": "utf8mb4"
}

# ================= Redis 配置 =================
r = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)
redis_key = "mcn:dedup"  # 已经包含历史数据

# ================= 入库参数 =================
input_file = "xhs_json.txt"
batch_size = 500
results = []

# ================= Step 1: 读取 JSON 文件并去重 =================
with open(input_file, "r", encoding="utf-8") as f:
    for line in tqdm(f, desc="读取文件"):
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue

        ext = data.get("extData", {})

        # ---------- 情况 1：单个 MCN ----------
        if isinstance(ext, dict) and "mcnId" in ext:
            mcnId = ext.get("mcnId")
            if not mcnId:
                continue
            mcnId_str = str(mcnId)
            if r.hexists(redis_key, mcnId_str):
                continue  # Redis 已存在，跳过
            r.hset(redis_key, mcnId_str, 1)  # 写入 Redis

            results.append((
                mcnId_str,
                ext.get("name", ""),
                ext.get("fullName", ""),
                ext.get("avatar", ""),
                (ext.get("desc") or "").replace("\n", " "),
                ext.get("fansCount") or 0,
                ext.get("anchorNum") or 0,
                2,
                "huitun"
            ))
            continue

        # ---------- 情况 2：list 中混达人 ----------
        for item in ext.get("list", []):
            mcnId = item.get("mcnId")
            if not mcnId:
                continue
            mcnId_str = str(mcnId)
            if r.hexists(redis_key, mcnId_str):
                continue
            r.hset(redis_key, mcnId_str, 1)

            results.append((
                mcnId_str,
                item.get("name", ""),
                item.get("fullName", ""),
                item.get("avatar", ""),
                (item.get("desc") or "").replace("\n", " "),
                item.get("fansCount") or 0,
                item.get("anchorNum") or 0,
                2,
                "huitun"
            ))

print(f"\n✅ 去重后需写入数据库的数据量：{len(results)} 条\n")

# ================= Step 2: 写入 MySQL =================
try:
    conn = pymysql.connect(**db_config, autocommit=False)
    cursor = conn.cursor()

    sql = """
    INSERT INTO mf_media_info_v3
    (mcn_mcnId, name, alias_name, mcn_avatar, mcn_desc, mcn_fansCount, mcn_anchorNum, media_type, source_type)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        name=VALUES(name),
        alias_name=VALUES(alias_name),
        mcn_avatar=VALUES(mcn_avatar),
        mcn_desc=VALUES(mcn_desc),
        mcn_fansCount=VALUES(mcn_fansCount),
        mcn_anchorNum=VALUES(mcn_anchorNum),
        media_type=VALUES(media_type),
        source_type=VALUES(source_type)
    """

    total = len(results)
    for i in tqdm(range(0, total, batch_size), desc="写入数据库"):
        batch = results[i:i + batch_size]
        cursor.executemany(sql, batch)
        conn.commit()

    print(f"✅ 成功写入 {total} 条数据到数据库")

except Exception as e:
    if conn:
        conn.rollback()
    print("❌ 写入数据库失败：", repr(e))

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()

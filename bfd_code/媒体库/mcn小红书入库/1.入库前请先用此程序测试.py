import json

input_file = "xhs_json.txt"
results = []

with open(input_file, "r", encoding="utf-8") as f:
    for lineno, line in enumerate(f, start=1):
        line = line.strip()
        if not line:
            continue

        try:
            data = json.loads(line)
        except:
            print(f"[WARN] 第 {lineno} 行 JSON 解析失败，跳过")
            continue

        ext = data.get("extData", {})

        # ✅ 情况 1：extData 是单个 MCN 结构
        if isinstance(ext, dict) and "mcnId" in ext:
            info = {
                "mcnId": ext.get("mcnId", ""),
                "name": ext.get("name", ""),
                "fullName": ext.get("fullName", ""),
                "avatar": ext.get("avatar", ""),
                "desc": (ext.get("desc") or "").replace("\n", " "),
                "fansCount": ext.get("fansCount", ""),
                "anchorNum": ext.get("anchorNum", "")
            }
            results.append(info)
            continue

        # ✅ 情况 2：extData.list 存在 → 先判断是不是 MCN 列表
        mcn_list = ext.get("list", [])
        if isinstance(mcn_list, list):
            for item in mcn_list:
                # ✅ 必须确认这是 MCN（达人没有 mcnId）
                if not item.get("mcnId"):
                    continue  # 跳过达人
                info = {
                    "mcnId": item.get("mcnId", ""),
                    "name": item.get("name", ""),
                    "fullName": item.get("fullName", ""),
                    "avatar": item.get("avatar", ""),
                    "desc": (item.get("desc") or "").replace("\n", " "),
                    "fansCount": item.get("fansCount", ""),
                    "anchorNum": item.get("anchorNum", "")
                }
                results.append(info)

# 输出
for r in results:
    print(r)

print(f"\n✅ 共提取到 {len(results)} 条 真实 MCN 数据")

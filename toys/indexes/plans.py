"""Print EXPLAIN QUERY PLAN for every captured statement, flagging scans and table fetches."""
import re, sqlite3, sys
store, stmts = sys.argv[1], sys.argv[2]
con = sqlite3.connect(store)
text = open(stmts).read()
blocks = re.findall(r"^== (.*?)\n(.*?)\n;;", text, re.S | re.M)
for label, sql in blocks:
    head = sql.split()[0].upper()
    if head not in ("SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "REPLACE"):
        continue
    n = max([int(x) for x in re.findall(r"\?(\d+)", sql)] or [0])
    try:
        rows = con.execute("EXPLAIN QUERY PLAN " + sql, [None] * n).fetchall()
    except Exception as e:
        print(f"## {label}\n  (plan failed: {e})\n  {sql[:200]}\n"); continue
    plan = [r[3] for r in rows]
    flag = [p for p in plan if p.startswith("SCAN") or ("USING INDEX" in p and "COVERING" not in p and "PRIMARY KEY" not in p) or "TEMP B-TREE" in p]
    print(f"## {label}  [{'; '.join(flag) if flag else 'seeks only'}]")
    print("  " + sql[:240].replace("\n", " "))
    for p in plan:
        print("    " + p)
    print()

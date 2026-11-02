"""A/B: picking each key's newest occurrence by a window (today) vs a per-key backward seek
(paper_trail: order desc, first), for the links of one node at now. Hearings plus aliases."""
import sqlite3, sys, time
store = sys.argv[1]
con = sqlite3.connect(store)
OCC = """(SELECT h.to_key, h.event AS name, h.axis, h.known, h.event FROM _hearings h
  WHERE h.entity='relation:wrote' AND h.line='main' AND h.key=:k AND h.to_key IS NOT NULL AND h.until IS NULL
  UNION ALL SELECT h.to_key, h.event, a.axis, a.known, a.alias FROM _hearings h CROSS JOIN _aliases a
  ON a.line=h.line AND a.identity=h.identity
  WHERE h.entity='relation:wrote' AND h.line='main' AND h.key=:k AND h.to_key IS NOT NULL AND h.until IS NULL)"""
WINDOW = f"""SELECT to_key, name, axis, known FROM (SELECT *, row_number() OVER (PARTITION BY to_key
  ORDER BY axis DESC, known DESC, event DESC) AS n FROM {OCC}) WHERE n = 1 ORDER BY to_key"""
SEEK = """SELECT t.to_key, (SELECT o.name FROM (SELECT h.event AS name, h.axis, h.known, h.event FROM _hearings h
    WHERE h.entity='relation:wrote' AND h.line='main' AND h.key=:k AND h.to_key=t.to_key AND h.until IS NULL
    UNION ALL SELECT h.event, a.axis, a.known, a.alias FROM _hearings h CROSS JOIN _aliases a
    ON a.line=h.line AND a.identity=h.identity
    WHERE h.entity='relation:wrote' AND h.line='main' AND h.key=:k AND h.to_key=t.to_key AND h.until IS NULL) o
    ORDER BY o.axis DESC, o.known DESC, o.event DESC LIMIT 1)
  FROM (SELECT DISTINCT to_key FROM _hearings WHERE entity='relation:wrote' AND line='main' AND key=:k) t ORDER BY t.to_key"""
for fan in (100, 1000, 10000):
    k = f"f{fan}"
    for name, sql in (("window", WINDOW), ("per-key seek", SEEK)):
        rows = con.execute(sql, {"k": k}).fetchall()
        runs = 5 if fan == 10000 else 20
        t = time.perf_counter()
        for _ in range(runs):
            con.execute(sql, {"k": k}).fetchall()
        ms = (time.perf_counter() - t) * 1000 / runs
        print(f"| {fan} | {name} | {ms:.3f} ms | {len(rows)} rows |")

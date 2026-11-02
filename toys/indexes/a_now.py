"""A: a current-version lookup table for reads at now, built once here from the window form,
then the links of one node read from it (seek + fetch the hearing) against the window form."""
import sqlite3, sys, time
con = sqlite3.connect(sys.argv[1])
t = time.perf_counter()
con.executescript("""
DROP TABLE IF EXISTS _now;
CREATE TABLE _now(entity TEXT NOT NULL, line TEXT NOT NULL, key TEXT NOT NULL, to_key TEXT NOT NULL,
  arrived INTEGER NOT NULL, axis BLOB NOT NULL, known INTEGER NOT NULL, event TEXT NOT NULL,
  PRIMARY KEY(entity, line, key, to_key)) WITHOUT ROWID;
CREATE INDEX _now_to ON _now(entity, line, to_key, key);
INSERT INTO _now SELECT entity, line, key, coalesce(to_key, ''), arrived, axis, known, event FROM (
  SELECT *, row_number() OVER (PARTITION BY entity, key, to_key ORDER BY axis DESC, known DESC, event DESC) AS n FROM (
    SELECT h.entity, h.line, h.key, h.to_key, h.arrived, h.axis, h.known, h.event FROM _hearings h WHERE h.until IS NULL
    UNION ALL SELECT h.entity, h.line, h.key, h.to_key, h.arrived, a.axis, a.known, a.alias FROM _hearings h
      CROSS JOIN _aliases a ON a.line = h.line AND a.identity = h.identity WHERE h.until IS NULL))
  WHERE n = 1;
""")
print(f"built _now in {time.perf_counter()-t:.1f} s, {con.execute('SELECT count(*) FROM _now').fetchone()[0]} rows")
NOW = """SELECT n.to_key, h.event, h.source, h.valid, n.axis, n.known, h.properties FROM _now n
  JOIN _hearings h ON h.arrived = n.arrived
  WHERE n.entity='relation:wrote' AND n.line='main' AND n.key=:k ORDER BY n.to_key"""
WINDOW = """SELECT to_key, name, source, valid, axis, known, properties FROM (SELECT *, row_number() OVER (PARTITION BY to_key
  ORDER BY axis DESC, known DESC, event DESC) AS n FROM (SELECT h.to_key, h.event AS name, h.source, h.valid, h.axis, h.known, h.event, h.properties FROM _hearings h
  WHERE h.entity='relation:wrote' AND h.line='main' AND h.key=:k AND h.to_key IS NOT NULL AND h.until IS NULL
  UNION ALL SELECT h.to_key, h.event, h.source, h.valid, a.axis, a.known, a.alias, h.properties FROM _hearings h CROSS JOIN _aliases a
  ON a.line=h.line AND a.identity=h.identity
  WHERE h.entity='relation:wrote' AND h.line='main' AND h.key=:k AND h.to_key IS NOT NULL AND h.until IS NULL)) WHERE n = 1 ORDER BY to_key"""
for fan in (100, 1000, 10000, 120000):
    for name, sql in (("window", WINDOW), ("A: _now lookup", NOW)):
        rows = con.execute(sql, {"k": f"f{fan}"}).fetchall()
        runs = 3 if fan >= 10000 else 20
        t = time.perf_counter()
        for _ in range(runs):
            con.execute(sql, {"k": f"f{fan}"}).fetchall()
        print(f"| {fan} | {name} | {(time.perf_counter()-t)*1000/runs:.3f} ms | {len(rows)} rows |")

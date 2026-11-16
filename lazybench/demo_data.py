"""Demo CSV generator and source mutator (to exercise mutation handling)."""

import csv
import os
import random

COLUMNS = ["id", "name", "email", "city", "country", "salary", "age",
           "dept", "joined", "score", "status", "notes"]
FIRST = ["ada", "grace", "alan", "edsger", "barbara", "donald", "frances",
         "margaret", "katherine", "radia", "linus", "guido"]
LAST = ["lovelace", "hopper", "turing", "dijkstra", "liskov", "knuth",
        "allen", "hamilton", "johnson", "perlman", "torvalds", "rossum"]
CITIES = ["lisbon", "osaka", "nairobi", "quito", "lyon", "hanoi", "perth"]
COUNTRIES = ["portugal", "japan", "kenya", "ecuador", "france", "vietnam",
             "australia"]
DEPTS = ["eng", "data", "ops", "research", "design"]
STATUS = ["active", "active", "active", "leave", "contractor"]


def generate(path, rows=100_000, seed=7):
    rng = random.Random(seed)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLUMNS)
        for i in range(rows):
            name = f"{rng.choice(FIRST)}{rng.choice(LAST)}{i % 997}"
            w.writerow([
                i,  # id doubles as the alignment canary: id == row_id
                name,
                f"{name}@example.com",
                rng.choice(CITIES),
                rng.choice(COUNTRIES),
                rng.randint(28000, 160000),
                rng.randint(21, 65),
                rng.choice(DEPTS),
                f"20{rng.randint(10, 25):02d}-{rng.randint(1, 12):02d}-"
                f"{rng.randint(1, 28):02d}",
                round(rng.uniform(0, 100), 2),
                rng.choice(STATUS),
                f"note-{rng.randint(0, 9999)}",
            ])
    return {"path": os.path.abspath(path), "rows": rows,
            "bytes": os.path.getsize(path)}


def mutate(path, mode):
    """Mutate a source file the way a producer system might mid-analysis."""
    with open(path, "rb") as f:
        lines = f.readlines()
    if mode == "append":
        with open(path, "ab") as f:
            for i in range(50):
                f.write(f"{900000 + i},newbie,newbie@x.com,perth,australia,"
                        f"50000,30,eng,2025-01-01,1.0,active,appended\n"
                        .encode())
        detail = "appended 50 rows"
    elif mode == "delete":
        cut = min(100, max(0, len(lines) - 2))
        del lines[len(lines) // 2: len(lines) // 2 + cut]
        with open(path, "wb") as f:
            f.writelines(lines)
        detail = f"deleted {cut} rows from the middle"
    elif mode == "edit":
        n = 0
        for i in range(1, len(lines), max(1, len(lines) // 37)):
            parts = lines[i].decode().rstrip("\n").split(",")
            if len(parts) > 5:
                parts[5] = "999999"
                lines[i] = (",".join(parts) + "\n").encode()
                n += 1
        with open(path, "wb") as f:
            f.writelines(lines)
        detail = f"edited salary on {n} rows"
    else:
        raise ValueError(f"unknown mutation mode {mode!r}")
    return {"path": os.path.abspath(path), "mode": mode, "detail": detail,
            "bytes": os.path.getsize(path)}

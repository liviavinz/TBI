from pathlib import Path

# Run this from the project root (where docker-compose.yml lives)
db = Path("data/tbi_database.sqlite")

for p in [db,
          db.with_suffix(db.suffix + "-wal"),
          db.with_suffix(db.suffix + "-shm"),
          db.with_suffix(db.suffix + "-journal")]:
    p.unlink(missing_ok=True)

print("Done.")

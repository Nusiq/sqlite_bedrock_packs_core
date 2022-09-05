from sqlite3 import Connection
from pathlib import Path


RESOURCE_PACK_BUILD_SCRIPT = '''
-- Resource Pack
CREATE TABLE ResourcePack (
    ResourcePack_pk INTEGER PRIMARY KEY AUTOINCREMENT,

    path Path NOT NULL
);
CREATE INDEX ResourcePack_path
ON ResourcePack (path);
'''

def load_resource_pack(db: Connection, rp_path: Path) -> int:
    count = db.execute(
        "SELECT total(path) FROM ResourcePack WHERE path = ?",
        (rp_path.as_posix(),)).fetchone()[0]
    if count != 0:
        raise ValueError("RP already loaded.")
    cursor = db.cursor()
    cursor.execute(
        "INSERT INTO ResourcePack (path) VALUES (?)",
        (rp_path.as_posix(),))
    return cursor.lastrowid

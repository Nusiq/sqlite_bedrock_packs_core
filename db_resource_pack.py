from sqlite3 import Connection
from pathlib import Path
from typing import Union

RESOURCE_PACK_BUILD_SCRIPT = '''
-- Resource Pack
CREATE TABLE ResourcePack (
    ResourcePack_pk INTEGER PRIMARY KEY AUTOINCREMENT,

    path Path NOT NULL
);
CREATE INDEX ResourcePack_path
ON ResourcePack (path);
'''

def load_resource_pack(db: Connection, rp_path: Union[Path, str]) -> int:
    if isinstance(rp_path, Path):
        rp_path = rp_path.as_posix()
    count = db.execute(
        "SELECT total(1) FROM ResourcePack WHERE path = ?",
        (rp_path,)).fetchone()[0]
    if count != 0:
        raise ValueError("RP already loaded.")
    cursor = db.cursor()
    cursor.execute(
        "INSERT INTO ResourcePack (path) VALUES (?)",
        (rp_path,))
    return cursor.lastrowid

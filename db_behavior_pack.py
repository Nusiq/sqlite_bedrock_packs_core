from sqlite3 import Connection
from pathlib import Path
from typing import Union

BEHAVIOR_PACK_BUILD_SCRIPT = '''
-- Behavior Pack
CREATE TABLE BehaviorPack (
    BehaviorPack_pk INTEGER PRIMARY KEY AUTOINCREMENT,

    path Path NOT NULL
);
CREATE INDEX BehaviorPack_path
ON BehaviorPack (path);
'''

def load_behavior_pack(db: Connection, rp_path: Union[Path, str]) -> int:
    if isinstance(rp_path, Path):
        rp_path = rp_path.as_posix()
    count = db.execute(
        "SELECT total(1) FROM BehaviorPack WHERE path = ?",
        (rp_path,)).fetchone()[0]
    if count != 0:
        raise ValueError("RP already loaded.")
    cursor = db.cursor()
    cursor.execute(
        "INSERT INTO BehaviorPack (path) VALUES (?)",
        (rp_path,))
    return cursor.lastrowid  # type: ignore

from typing import cast
from sqlite3 import Connection
from pathlib import Path
from .better_json_tools import load_jsonc
import json

BP_ANIMATION_BUILD_SCRIPT = '''
-- BpAnimation
CREATE TABLE BpAnimationFile (
    BpAnimationFile_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    BehaviorPack_fk INTEGER,

    path Path NOT NULL,
    FOREIGN KEY (BehaviorPack_fk) REFERENCES BehaviorPack (BehaviorPack_pk)
        ON DELETE CASCADE
);
CREATE INDEX BpAnimationFile_BehaviorPack_fk
ON BpAnimationFile (BehaviorPack_fk);

CREATE TABLE BpAnimation (
    BpAnimation_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    BpAnimationFile_fk INTEGER NOT NULL,

    identifier TEXT NOT NULL,
    jsonPath TEXT NOT NULL,
    
    FOREIGN KEY (BpAnimationFile_fk) REFERENCES BpAnimationFile (BpAnimationFile_pk)
        ON DELETE CASCADE
);
CREATE INDEX BpAnimation_BpAnimationFile_fk
ON BpAnimation (BpAnimationFile_fk);

'''

def load_bp_animations(db: Connection, bp_id: int):
    bp_path: Path = db.execute(
        "SELECT path FROM BehaviorPack WHERE BehaviorPack_pk = ?",
        (bp_id,)
    ).fetchone()[0]

    for animation_path in (bp_path / "animations").rglob("*.json"):
        load_bp_animation(db, animation_path, bp_id)

def load_bp_animation(db: Connection, animation_path: Path, bp_id: int):
    cursor = db.cursor()
    # BP ANIMATION FILE
    cursor.execute(
        "INSERT INTO BpAnimationFile (path, BehaviorPack_fk) VALUES (?, ?)",
        (animation_path.as_posix(), bp_id)
    )
    file_pk = cursor.lastrowid
    try:
        animations_walker = load_jsonc(animation_path)
    except json.JSONDecodeError:
        # sinlently skip invalid files. The file is in db but has no data
        return

    for animation_walker in animations_walker / "animations" // str:
        identifier_data: str = cast(str, animation_walker.parent_key)
        if not identifier_data.startswith("animation."):
            continue
        cursor.execute(
            '''
            INSERT INTO BpAnimation (
                BpAnimationFile_fk, identifier, jsonPath
            ) VALUES (?, ?, ?)
            ''',
            (file_pk, identifier_data, animation_walker.path_str)
        )
        # bp_anim_pk = cursor.lastrowid

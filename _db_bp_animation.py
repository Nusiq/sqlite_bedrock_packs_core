from typing import cast
from sqlite3 import Connection
from pathlib import Path
from .better_json_tools import load_jsonc
from ._views import dbtableview
import json


@dbtableview(
    properties={
        "path": (Path, "NOT NULL")
    },
    connects_to=["BehaviorPack"]
)
class BpAnimationFile: ...
 
@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL")
    },
    connects_to=["BpAnimationFile"]
)
class BpAnimation: ...

BP_ANIMATION_BUILD_SCRIPT: str = (
    BpAnimationFile.build_script +
    BpAnimation.build_script
)

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

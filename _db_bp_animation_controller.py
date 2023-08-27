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
class BpAnimationControllerFile: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL")
    },
    connects_to=["BpAnimationControllerFile"]
)
class BpAnimationController: ...


BP_ANIMATION_CONTROLLER_BUILD_SCRIPT: str = (
    BpAnimationControllerFile.build_script +
    BpAnimationController.build_script
)

def load_bp_animation_controllers(db: Connection, bp_id: int):
    bp_path: Path = db.execute(
        "SELECT path FROM BehaviorPack WHERE BehaviorPack_pk = ?",
        (bp_id,)
    ).fetchone()[0]

    for ac_path in (bp_path / "animation_controllers").rglob("*.json"):
        load_bp_animation_controller(db, ac_path, bp_id)


def load_bp_animation_controller(db: Connection, animation_controller_path: Path, bp_id: int):
    cursor = db.cursor()
    # BP ANIMATION FILE
    cursor.execute(
        "INSERT INTO BpAnimationControllerFile (path, BehaviorPack_fk) VALUES (?, ?)",
        (animation_controller_path.as_posix(), bp_id)
    )
    file_pk = cursor.lastrowid
    try:
        acs_walker = load_jsonc(animation_controller_path)
    except json.JSONDecodeError:
        # sinlently skip invalid files. The file is in db but has no data
        return

    for ac_walker in acs_walker / "animation_controllers" // str:
        identifier_data: str = cast(str, ac_walker.parent_key)
        if not identifier_data.startswith("controller.animation."):
            continue
        cursor.execute(
            '''
            INSERT INTO BpAnimationController (
                BpAnimationControllerFile_fk, identifier, jsonPath
            ) VALUES (?, ?, ?)
            ''',
            (file_pk, identifier_data, ac_walker.path_str)
        )
        # bp_anim_pk = cursor.lastrowid


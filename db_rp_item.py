from typing import cast, Optional
from sqlite3 import Connection
from pathlib import Path
from .better_json_tools import load_jsonc
from .utils import parse_format_version
from .decorators import dbtableview
import json

@dbtableview(
    properties={
        "path": (Path, "NOT NULL")
    },
    connects_to=["ResourcePack"]
)
class RpItemFile: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "icon": (str, "")
    },
    connects_to=["RpItemFile"]
)
class RpItem: ...

RP_ITEM_BUILD_SCRIPT = (
    RpItemFile.build_script +
    RpItem.build_script
)

def load_rp_items(db: Connection, rp_id: int):
    rp_path: Path = db.execute(
        "SELECT path FROM ResourcePack WHERE ResourcePack_pk = ?",
        (rp_id,)
    ).fetchone()[0]

    for item_path in (rp_path / "items").rglob("*.json"):
        load_rp_item(db, item_path, rp_id)

def load_rp_item(db: Connection, item_path: Path, rp_id: int):
    cursor = db.cursor()
    # RP ITEM FILE
    cursor.execute(
        "INSERT INTO RpItemFile (path, ResourcePack_fk) VALUES (?, ?)",
        (item_path.as_posix(), rp_id)
    )
    file_pk = cursor.lastrowid
    try:
        item_walker = load_jsonc(item_path)
    except json.JSONDecodeError:
        # sinlently skip invalid files. The file is in db but has no data
        return

    # RP ITEM
    identifier = item_walker / "minecraft:item" / "description" / "identifier"
    if not isinstance(identifier.data, str):
        return  # Silently skip items without an identifier

    icon: Optional[str] = None
    icon_walker = (
        item_walker / "minecraft:item" / "components" / "minecraft:icon")
    if isinstance(icon_walker.data, str):
        icon = icon_walker.data
    cursor.execute(
        '''
        INSERT INTO RpItem (
            RpItemFile_fk, identifier, icon
        ) VALUES (?, ?, ?)
        ''',
        (file_pk, identifier.data, icon)
    )
    # rp_anim_pk = cursor.lastrowid

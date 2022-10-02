from typing import cast, Optional
from sqlite3 import Connection
from pathlib import Path
from .better_json_tools import load_jsonc
from .utils import parse_format_version
import json

BP_ITEM_BUILD_SCRIPT = '''
-- BpItem
CREATE TABLE BpItemFile (
    BpItemFile_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    BehaviorPack_fk INTEGER,

    path Path NOT NULL,
    FOREIGN KEY (BehaviorPack_fk) REFERENCES BehaviorPack (BehaviorPack_pk)
        ON DELETE CASCADE
);
CREATE INDEX BpItemFile_BehaviorPack_fk
ON BpItemFile (BehaviorPack_fk);


CREATE TABLE BpItemParserVersionEnum (
    -- Emulates enum type. Stores possible value of BpItem.parserVersion
    --- column.
    value TEXT PRIMARY KEY
);
INSERT INTO BpItemParserVersionEnum (value) VALUES ('1.10'), ('1.16.100');

CREATE TABLE BpItem (
    BpItem_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    BpItemFile_fk INTEGER NOT NULL,

    identifier TEXT NOT NULL,
    -- The version of the parser. The value is based on fromat_version property
    -- of the file or matching schema (when format_version is missing).
    -- It divides the items into two groups 1.10 and 1.16.100. Most of the
    -- format_versions in Minecraft don't change the format so you need only
    -- two parsers to handle all the items.
    parserVersion TEXT NOT NULL,
    -- The texture of the item. It only exists in 1.16.100+ items.
    texture TEXT,
    
    FOREIGN KEY (BpItemFile_fk) REFERENCES BpItemFile (BpItemFile_pk)
        ON DELETE CASCADE
    -- Constraint emulates enum
    FOREIGN KEY (parserVersion) REFERENCES BpItemParserVersionEnum (value)
);
CREATE INDEX BpItem_BpItemFile_fk
ON BpItem (BpItemFile_fk);
'''

def load_bp_items(db: Connection, bp_id: int):
    bp_path: Path = db.execute(
        "SELECT path FROM BehaviorPack WHERE BehaviorPack_pk = ?",
        (bp_id,)
    ).fetchone()[0]

    for item_path in (bp_path / "items").rglob("*.json"):
        load_bp_item(db, item_path, bp_id)

def load_bp_item(db: Connection, item_path: Path, bp_id: int):
    cursor = db.cursor()
    # BP ITEM FILE
    cursor.execute(
        "INSERT INTO BpItemFile (path, BehaviorPack_fk) VALUES (?, ?)",
        (item_path.as_posix(), bp_id)
    )
    file_pk = cursor.lastrowid
    try:
        item_walker = load_jsonc(item_path)
    except json.JSONDecodeError:
        # sinlently skip invalid files. The file is in db but has no data
        return

    # BP ITEM
    identifier = item_walker / "minecraft:item" / "description" / "identifier"
    if not isinstance(identifier.data, str):
        return  # Silently skip items without an identifier

    format_version_walker = item_walker / "format_version"
    parser_version: Optional[str] = None
    texture: Optional[str] = None
    if isinstance(format_version_walker.data, str):
        try:
            format_version_tuple = parse_format_version(format_version_walker.data)
            if format_version_tuple >= (1, 16, 100):
                parser_version = "1.16.100"
            else:
                parser_version = "1.10"
        except ValueError:
            pass
    # Parse texture if not 1.10
    if parser_version is None or parser_version == "1.16.100":
        texture_walker = (
            item_walker / "minecraft:item" / "components" / "minecraft:icon" /
            "texture")
        if isinstance(texture_walker.data, str):
            texture = texture_walker.data
    # If version unknown, base it on existance of texture
    if parser_version is None:
        if texture is None:
            parser_version = "1.10"
        else:
            parser_version = "1.16.100"
    cursor.execute(
        '''
        INSERT INTO BpItem (
            BpItemFile_fk, identifier, parserVersion, texture
        ) VALUES (?, ?, ?, ?)
        ''',
        (file_pk, identifier.data, parser_version, texture)
    )
    # bp_anim_pk = cursor.lastrowid

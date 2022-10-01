from typing import cast, Optional
from sqlite3 import Connection
from pathlib import Path
from .better_json_tools import load_jsonc
import json

LOOT_TABLE_BUILD_SCRIPT = '''
-- Loot Table
CREATE TABLE LootTableFile (
    LootTableFile_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    BehaviorPack_fk INTEGER,

    path Path NOT NULL,
    FOREIGN KEY (BehaviorPack_fk) REFERENCES BehaviorPack (BehaviorPack_pk)
        ON DELETE CASCADE
);
CREATE INDEX LootTableFile_BehaviorPack_fk
ON LootTableFile (BehaviorPack_fk);

CREATE TABLE LootTable (
    LootTable_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    LootTableFile_fk INTEGER NOT NULL,

    -- Identifier of the loot table (path to the file relative to the behavior
    -- pack root). Unike some other path based identifiers, this one includes
    -- the file extension.
    identifier TEXT NOT NULL,

    FOREIGN KEY (LootTableFile_fk) REFERENCES LootTableFile (LootTableFile_pk)
        ON DELETE CASCADE
);
CREATE INDEX LootTable_LootTableFile_fk
ON LootTable (LootTableFile_fk);

CREATE TABLE LootTableItemField (
    -- A reference to an item inside a loot table.

    LootTableItemField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    LootTable_fk INTEGER NOT NULL,

    identifier TEXT NOT NULL,
    jsonPath TEXT NOT NULL,

    FOREIGN KEY (LootTable_fk) REFERENCES LootTable (LootTable_pk)
        ON DELETE CASCADE
);
CREATE INDEX LootTableItemField_LootTable_fk
ON LootTableItemField (LootTable_fk);

CREATE TABLE LootTableLootTableField (
    -- A reference to an loot table inside another loot table.

    LootTableLootTableField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    LootTable_fk INTEGER NOT NULL,

    identifier TEXT NOT NULL,
    jsonPath TEXT NOT NULL,

    FOREIGN KEY (LootTable_fk) REFERENCES LootTable (LootTable_pk)
        ON DELETE CASCADE
);
CREATE INDEX LootTableLootTableField_LootTable_fk
ON LootTableLootTableField (LootTable_fk);
'''

def load_loot_tables(db: Connection, rp_id: int):
    rp_path: Path = db.execute(
        "SELECT path FROM BehaviorPack WHERE BehaviorPack_pk = ?",
        (rp_id,)
    ).fetchone()[0]

    for loot_table_path in (rp_path / "loot_tables").rglob("*.json"):
        load_loot_table(db, loot_table_path, rp_path, rp_id)

def load_loot_table(
        db: Connection, loot_table_path: Path, rp_path: Path, rp_id: int):
    cursor = db.cursor()
    # LOOT TABLE FILE
    cursor.execute(
        "INSERT INTO LootTableFile (path, BehaviorPack_fk) VALUES (?, ?)",
        (loot_table_path.as_posix(), rp_id)
    )
    file_pk = cursor.lastrowid
    try:
        loot_table_jsonc = load_jsonc(loot_table_path)
    except json.JSONDecodeError:
        # sinlently skip invalid files. The file is in db but has no data
        return

    # LOOT TABLE
    identifier = loot_table_path.relative_to(rp_path).as_posix()
    cursor.execute(
        '''
        INSERT INTO LootTable (
            identifier, LootTableFile_fk
        ) VALUES (?, ?)
        ''',
        (identifier, file_pk)
    )
    loot_table_pk = cursor.lastrowid

    # LOOT TABLE ITEM FIELDS
    entry_walker = loot_table_jsonc / "pools" // int / "entries" // int
    while len(entry_walker) > 0:
        for ew in entry_walker:
            ew_name = ew / "name"
            if not isinstance(ew_name.data, str):
                continue
            ew_type = ew / "type"
            if ew_type.data == "item":
                cursor.execute(
                    '''
                    INSERT INTO LootTableItemField (
                        identifier, jsonPath, LootTable_fk
                    ) VALUES (?, ?, ?)
                    ''',
                    (ew_name.data, ew_name.path_str, loot_table_pk)
                )
            elif ew_type.data == "loot_table":
                cursor.execute(
                    '''
                    INSERT INTO LootTableLootTableField (
                        identifier, jsonPath, LootTable_fk
                    ) VALUES (?, ?, ?)
                    ''',
                    (ew_name.data, ew_name.path_str, loot_table_pk)
                )
        # Entry property can have pools. This is a nested structure.
        entry_walker = entry_walker / "pools" // int / "entries" // int

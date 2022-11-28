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

CREATE TABLE LootTableItemSpawnEggReferenceFieldConnectinTypeEnum (
    -- This table is used to store possible values for the
    -- LootTableItemSpawnEggReferenceField.connectionType column.
    -- Stores the type of connection, it can be either "direct" or
    -- "set_actor_id_function".
    value TEXT PRIMARY KEY
);
INSERT INTO LootTableItemSpawnEggReferenceFieldConnectinTypeEnum (value)
VALUES ("direct"), ("set_actor_id_function");

CREATE TABLE LootTableItemSpawnEggReferenceField (
    -- A reference to a spawn egg inside an item inside a loot table.

    LootTableItemSpawnEggReferenceField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    LootTableItemField_fk INTEGER NOT NULL,

    entityIdentifier TEXT NOT NULL,
    spawnEggIdentifier TEXT NOT NULL,
    connectionType TEXT NOT NULL,

    -- If connectionType is direct, it points at the identifier
    jsonPath TEXT NOT NULL,

    FOREIGN KEY (LootTableItemField_fk) REFERENCES LootTableItemField (LootTableItemField_pk)
        ON DELETE CASCADE
    -- Constraint emulates enum
    FOREIGN KEY (connectionType) REFERENCES
        LootTableItemSpawnEggReferenceFieldConnectinTypeEnum (value)
);
CREATE INDEX LootTableItemSpawnEggReferenceField_LootTableItemField_fk
ON LootTableItemSpawnEggReferenceField (LootTableItemField_fk);

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

    # LOOT TABLE ITEM & LOOT TABLE FIELDS
    entry_walker = loot_table_jsonc / "pools" // int / "entries" // int
    while len(entry_walker) > 0:
        for ew in entry_walker:
            ew_name = ew / "name"
            if not isinstance(ew_name.data, str):
                continue
            entry_name = ew_name.data
            ew_type = ew / "type"
            if ew_type.data == "item":
                # ITEM
                cursor.execute(
                    '''
                    INSERT INTO LootTableItemField (
                        identifier, jsonPath, LootTable_fk
                    ) VALUES (?, ?, ?)
                    ''',
                    (entry_name, ew_name.path_str, loot_table_pk)
                )
                item_pk = cursor.lastrowid
                # ITEM SPAWN EGG REFERENCE
                if entry_name.endswith("_spawn_egg"):
                    # DIRECT REFERENCE
                    cursor.execute(
                        '''
                        INSERT INTO LootTableItemSpawnEggReferenceField (
                            entityIdentifier, spawnEggIdentifier,
                            connectionType, jsonPath, LootTableItemField_fk
                        ) VALUES (?, ?, ?, ?, ?)
                        ''',
                        (
                            entry_name[:-10],  # remove "_spawn_egg"
                            entry_name,
                            "direct",
                            ew_name.path_str,
                            item_pk,
                        )
                    )
                elif entry_name == 'minecraft:spawn_egg':
                    # REFERENCE USING set_actor_id FUNCTION
                    functions_walker = ew / "functions" // int
                    for fw in functions_walker:
                        function_name = fw / "function"
                        if not (
                                isinstance(function_name.data, str) and 
                                function_name.data == "set_actor_id"):
                            continue
                        entity_identifier = fw / "id"
                        if not isinstance(entity_identifier.data, str):
                            continue
                        cursor.execute(
                            '''
                            INSERT INTO LootTableItemSpawnEggReferenceField (
                                entityIdentifier, spawnEggIdentifier,
                                connectionType, jsonPath, LootTableItemField_fk
                            ) VALUES (?, ?, ?, ?, ?)
                            ''',
                            (
                                entity_identifier.data,
                                entity_identifier.data + "_spawn_egg",
                                "set_actor_id_function",
                                entity_identifier.path_str,
                                item_pk,
                            )
                        )
            elif ew_type.data == "loot_table":
                # LOOT TABLE
                cursor.execute(
                    '''
                    INSERT INTO LootTableLootTableField (
                        identifier, jsonPath, LootTable_fk
                    ) VALUES (?, ?, ?)
                    ''',
                    (entry_name, ew_name.path_str, loot_table_pk)
                )
        # Entry property can have pools. This is a nested structure.
        entry_walker = entry_walker / "pools" // int / "entries" // int

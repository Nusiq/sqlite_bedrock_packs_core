from typing import cast, Optional
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
class LootTableFile: ...

@dbtableview(
    properties={
        # Identifier of the loot table(path to the file relative to the behavior
        # pack root). Unike some other path based identifiers, this one includes
        # the file extension.
        "identifier": (str, "NOT NULL"),
    },
    connects_to=["LootTableFile"]
)
class LootTable: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL")
    },
    connects_to=["LootTable"],
    weak_connects_to=[
        ("identifier", "RpItem", "identifier"),
        ("identifier", "BpItem", "identifier")
    ]
)
class LootTableItemField: ...

@dbtableview(
    properties={
        "entityIdentifier": (str, "NOT NULL"),
        "spawnEggIdentifier": (str, "NOT NULL"),

        # If connectionType is direct, it points at the identifier
        "jsonPath": (str, "NOT NULL")
    },
    enum_properties={
        # This table is used to store possible values for the
        # LootTableItemSpawnEggReferenceField.connectionType column.
        # Stores the type of connection, it can be either "direct" or
        # "set_actor_id_function".
        "connectionType": ["direct", "set_actor_id_function"]
    },
    connects_to=["LootTableItemField"],
    weak_connects_to=[
        ("spawnEggIdentifier", "EntitySpawnEggField", "identifier")
    ]
)
class LootTableItemSpawnEggReferenceField: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL")
    },
    connects_to=["LootTable"],
    weak_connects_to=[
        ("identifier", "LootTable", "identifier")
    ]
)
class LootTableLootTableField: ...

LOOT_TABLE_BUILD_SCRIPT = (
    LootTableFile.build_script +
    LootTable.build_script +
    LootTableItemField.build_script +
    LootTableItemSpawnEggReferenceField.build_script +
    LootTableLootTableField.build_script
)

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
                                fw.path_str,
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

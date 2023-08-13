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
class EntityFile: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
    },
    connects_to=["EntityFile"]
)
class Entity: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL"),
    },
    enum_properties={
        "componentType": ["minecraft:loot", "minecraft:equipment"]
    },
    connects_to=["Entity"],
    weak_connects_to=[
        ("identifier", "LootTable", "identifier")
    ]
)
class EntityLootField: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL"),
    },
    enum_properties={
        "componentType": ["minecraft:economy_trade_table", "minecraft:trade_table"]
    },
    connects_to=["Entity"],
    weak_connects_to=[
        ("identifier", "TradeTable", "identifier")
    ]
)
class EntityTradeField: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
    },
    connects_to=["Entity"]
)
class EntitySpawnEggField:
    '''
    Spawn eggs are added to the database based on entities that use the
    is_spawnable property. The name of the spawn egg is based on the entity
    identifier.
    '''

ENTITY_BUILD_SCRIPT = (
    EntityFile.build_script +
    Entity.build_script +
    EntityLootField.build_script +
    EntityTradeField.build_script +
    EntitySpawnEggField.build_script
)

def load_entities(db: Connection, bp_id: int):
    bp_path: Path = db.execute(
        "SELECT path FROM BehaviorPack WHERE BehaviorPack_pk = ?",
        (bp_id,)
    ).fetchone()[0]

    for entity_path in (bp_path / "entities").rglob("*.json"):
        load_entity(db, entity_path, bp_id)

def load_entity(db: Connection, entity_path: Path, bp_id: int):
    cursor = db.cursor()
    # ENTITY FILE
    cursor.execute(
        "INSERT INTO EntityFile (path, BehaviorPack_fk) VALUES (?, ?)",
        (entity_path.as_posix(), bp_id))

    file_pk = cursor.lastrowid
    try:
        entity_jsonc = load_jsonc(entity_path)
    except json.JSONDecodeError:
        # sinlently skip invalid files. The file is in db but has no data
        return
    entity_walker = entity_jsonc / "minecraft:entity"
    description = entity_walker / "description"

    # ENTITY - IDENTIFIER
    entity_identifier = (description / "identifier").data
    if not isinstance(entity_identifier, str):
        # Skip entitites without identifiers
        return
    cursor.execute(
        '''
        INSERT INTO Entity (
        identifier, EntityFile_fk
        ) VALUES (?, ?)
        ''',
        (entity_identifier, file_pk))
    entity_pk = cursor.lastrowid


    all_componet_groups = (
        entity_walker / "component_groups" // str+
        entity_walker / "components"
    )
    # LOOT
    loot_table_walkers = (
        all_componet_groups / "minecraft:loot" / "table" +
        all_componet_groups / "minecraft:equipment" / "table"
    )
    for loot_table_walker in loot_table_walkers:
        loot_table = loot_table_walker.data
        if not isinstance(loot_table, str):
            continue
        cursor.execute(
            '''
            INSERT INTO EntityLootField (
            identifier, jsonPath, Entity_fk, componentType
            ) VALUES (?, ?, ?, ?)
            ''',
            (
                loot_table,
                loot_table_walker.path_str,
                entity_pk,

                # minecraft:loot OR minecraft:equipment
                loot_table_walker.parent.parent_key
            )
        )

    # TRADE
    trade_table_walkers = (
        all_componet_groups / "minecraft:economy_trade_table" / "table" +
        all_componet_groups / "minecraft:trade_table" / "table"
    )
    for trade_table_walker in trade_table_walkers:
        trade_table = trade_table_walker.data
        if not isinstance(trade_table, str):
            continue
        cursor.execute(
            '''
            INSERT INTO EntityTradeField (
            identifier, jsonPath, Entity_fk, componentType
            ) VALUES (?, ?, ?, ?)
            ''',
            (
                trade_table,
                trade_table_walker.path_str,
                entity_pk,

                # minecraft:trade_table OR minecraft:economy_trade_table
                trade_table_walker.parent.parent_key
            )
        )
    # SPAWN EGG
    spawn_egg_identifier = f'{entity_identifier}_spawn_egg'
    spawnable = (description / "is_spawnable").data
    if isinstance(spawnable, bool) and spawnable:
        cursor.execute(
            '''
            INSERT INTO EntitySpawnEggField (
            identifier, Entity_fk
            ) VALUES (?, ?)
            ''',
            (
                spawn_egg_identifier,
                entity_pk
            )
        )

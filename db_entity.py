from sqlite3 import Connection
from pathlib import Path
from .better_json_tools import load_jsonc
import json

ENTITY_BUILD_SCRIPT = '''
-- Behavior pack entity file & content
CREATE TABLE EntityFile (
    EntityFile_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    BehaviorPack_fk INTEGER,

    path Path NOT NULL,
    FOREIGN KEY (BehaviorPack_fk) REFERENCES BehaviorPack (BehaviorPack_pk)
        ON DELETE CASCADE
);
CREATE INDEX EntityFile_BehaviorPack_fk
ON EntityFile (BehaviorPack_fk);

CREATE TABLE Entity (
    Entity_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    EntityFile_fk INTEGER NOT NULL,

    identifier TEXT NOT NULL,
    FOREIGN KEY (EntityFile_fk) REFERENCES EntityFile (EntityFile_pk)
        ON DELETE CASCADE
);
CREATE INDEX Entity_EntityFile_fk
ON Entity (EntityFile_fk);

CREATE TABLE EntityLootFieldComponentTypeEnum (
    -- This table is used to store possible values for the 
    -- EntityLootField.componentType column. Stores the names of the
    -- components that can have a loot table reference (like 
    -- "minecraft:loot" or "minecraft:equipment")
    value TEXT PRIMARY KEY
);
INSERT INTO EntityLootFieldComponentTypeEnum (value)
VALUES ("minecraft:loot"), ("minecraft:equipment");


CREATE TABLE EntityLootField (
    EntityLootField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    Entity_fk INTEGER NOT NULL,

    identifier TEXT NOT NULL,
    jsonPath TEXT NOT NULL,
    componentType TEXT NOT NULL,

    FOREIGN KEY (Entity_fk) REFERENCES Entity (Entity_pk)
        ON DELETE CASCADE
    -- Constraint emulates enum
    FOREIGN KEY (componentType) REFERENCES EntityLootFieldComponentTypeEnum (value)
);
CREATE INDEX EntityLootField_Entity_fk
ON EntityLootField (Entity_fk);

CREATE TABLE EntityTradeFieldComponentTypeEnum (
    -- This table is used to store possible values for the
    -- EntityTradeField.componentType column. Stores the names of the
    -- components that can have a trade table reference (like
    -- "minecraft:economy_trade_table" or "minecraft:trade_table")
    value TEXT PRIMARY KEY
);
INSERT INTO EntityTradeFieldComponentTypeEnum (value)
VALUES ("minecraft:economy_trade_table"), ("minecraft:trade_table");

CREATE TABLE EntityTradeField (
    EntityTradeField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    Entity_fk INTEGER NOT NULL,

    identifier TEXT NOT NULL,
    jsonPath TEXT NOT NULL,
    componentType TEXT NOT NULL,

    FOREIGN KEY (Entity_fk) REFERENCES Entity (Entity_pk)
        ON DELETE CASCADE
    -- Constraint emulates enum
    FOREIGN KEY (componentType) REFERENCES EntityTradeFieldComponentTypeEnum (value)
);
CREATE INDEX EntityTradeField_Entity_fk
ON EntityTradeField (Entity_fk);

CREATE TABLE EntitySpawnEggField (
    -- Spawn eggs are added to the database based on entities that use the
    -- is_spawnable property. The name of the spawn egg is based on the entity
    -- identifier.

    EntitySpawnEggField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    Entity_fk INTEGER NOT NULL,

    identifier TEXT NOT NULL,

    FOREIGN KEY (Entity_fk) REFERENCES Entity (Entity_pk)
        ON DELETE CASCADE
);
CREATE INDEX EntitySpawnEggField_Entity_fk
ON EntitySpawnEggField (Entity_fk);

'''

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

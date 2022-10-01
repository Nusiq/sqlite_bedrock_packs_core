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
    description = entity_jsonc / "minecraft:entity" / "description"

    # ENTITY - IDENTIFIER
    identifier = (description / "identifier").data
    if not isinstance(identifier, str):
        # Skip entitites without identifiers
        return
    cursor.execute(
        '''
        INSERT INTO Entity (
        identifier, EntityFile_fk
        ) VALUES (?, ?)
        ''',
        (identifier, file_pk))
    entity_pk = cursor.lastrowid

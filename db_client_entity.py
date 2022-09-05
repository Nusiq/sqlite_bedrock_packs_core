from sqlite3 import Connection
from pathlib import Path
from .better_json import load_jsonc
import json

CLIENT_ENTITY_BUILD_SCRIPT = '''
-- Resource pack entity file & content
CREATE TABLE ClientEntityFile (
    ClientEntityFile_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    ResourcePack_fk INTEGER,

    path Path,
    FOREIGN KEY (ResourcePack_fk) REFERENCES ResourcePack (ResourcePack_pk)
        ON DELETE CASCADE
);
CREATE INDEX ClientEntityFile_ResourcePack_fk
ON ClientEntityFile (ResourcePack_fk);

CREATE TABLE ClientEntity (
    ClientEntity_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    ClientEntityFile_fk INTEGER NOT NULL,

    identifier TEXT,
    FOREIGN KEY (ClientEntityFile_fk) REFERENCES ClientEntityFile (ClientEntityFile_pk)
        ON DELETE CASCADE
);
CREATE INDEX ClientEntity_ClientEntityFile_fk
ON ClientEntity (ClientEntityFile_fk);

CREATE TABLE ClientEntityRenderControllerField (
    ClientEntityRenderControllerField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    ClientEntity_fk INTEGER NOT NULL,

    identifier TEXT,
    condition TEXT,
    jsonPath TEXT,

    FOREIGN KEY (ClientEntity_fk) REFERENCES ClientEntity (ClientEntity_pk)
        ON DELETE CASCADE
);
CREATE INDEX ClientEntityRenderControllerField_ClientEntity_fk
ON ClientEntityRenderControllerField (ClientEntity_fk);

CREATE TABLE ClientEntityGeometryField (
    ClientEntityGeometryField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    ClientEntity_fk INTEGER NOT NULL,

    shortName TEXT,
    identifier TEXT,
    jsonPath TEXT,
    
    FOREIGN KEY (ClientEntity_fk) REFERENCES ClientEntity (ClientEntity_pk)
        ON DELETE CASCADE
);
CREATE INDEX ClientEntityGeometryField_ClientEntity_fk
ON ClientEntityGeometryField (ClientEntity_fk);

CREATE TABLE ClientEntityTextureField (
    ClientEntityTextureField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    ClientEntity_fk INTEGER NOT NULL,


    shortName TEXT,
    -- identifier is the path without the extension
    identifier TEXT,
    jsonPath TEXT,

    FOREIGN KEY (ClientEntity_fk) REFERENCES ClientEntity (ClientEntity_pk)
        ON DELETE CASCADE
);
CREATE INDEX ClientEntityTextureField_ClientEntity_fk
ON ClientEntityTextureField (ClientEntity_fk);


CREATE TABLE ClientEntityMaterialField (
    ClientEntityMaterialField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    ClientEntity_fk INTEGER NOT NULL,

    shortName TEXT,
    identifier TEXT,
    jsonPath TEXT,

    FOREIGN KEY (ClientEntity_fk) REFERENCES ClientEntity (ClientEntity_pk)
        ON DELETE CASCADE
);
CREATE INDEX ClientEntityMaterialField_ClientEntity_fk
ON ClientEntityMaterialField (ClientEntity_fk);
'''

def load_client_entities(db: Connection, rp_id: int):
    rp_path: Path = db.execute(
        "SELECT path FROM ResourcePack WHERE ResourcePack_pk = ?",
        (rp_id,)
    ).fetchone()[0]

    for entity_path in (rp_path / "entity").rglob("*.json"):
        load_client_entity(db, entity_path, rp_id)

def load_client_entity(db: Connection, entity_path: Path, rp_id: int):
    cursor = db.cursor()
    # ENTITY FILE
    cursor.execute(
        "INSERT INTO ClientEntityFile (path, ResourcePack_fk) VALUES (?, ?)",
        (entity_path.as_posix(), rp_id))

    file_pk = cursor.lastrowid
    try:
        entity_jsonc = load_jsonc(entity_path)
    except json.JSONDecodeError:
        # sinlently skip invalid files. The file is in db but has no data
        return
    description = entity_jsonc / "minecraft:client_entity" / "description"

    # ENTITY - IDENTIFIER
    identifier = (description / "identifier").data
    if not isinstance(identifier, str):
        identifier = None
    cursor.execute(
        '''
        INSERT INTO ClientEntity (
        identifier, ClientEntityFile_fk
        ) VALUES (?, ?)
        ''',
        (identifier, file_pk))
    entity_pk = cursor.lastrowid
    # RENDER CONTROLLERS - unconditional
    for rc in (description / "render_controllers" // int):
        if isinstance(rc.data, str):
            identifier = rc.data
        else:
            continue  # Probably conditional render controller
        cursor.execute(
            '''
            INSERT INTO ClientEntityRenderControllerField (
                ClientEntity_fk, identifier, jsonPath
            ) VALUES (?, ?, ?)
            ''',
            (entity_pk, identifier, rc.path_str))
    # RENDER CONTROLLERS - conditional
    for rc in (description / "render_controllers" // int // str):
        if isinstance(rc.data, str):
           condition = rc.data
        else:
            condition = None
        cursor.execute(
            '''
            INSERT INTO ClientEntityRenderControllerField (
                ClientEntity_fk, identifier, condition, jsonPath
            ) VALUES (?, ?, ?, ?)
            ''',
            (entity_pk, rc.parent_key, condition, rc.path_str)
        )
    # MATERIALS
    for material in description / "materials" // str:
        if isinstance(material.data, str):
            identifier = material.data
        else:
            identifier = None
        cursor.execute(
            '''
            INSERT INTO ClientEntityMaterialField (
                ClientEntity_fk, shortName, identifier, jsonPath
            ) VALUES (?, ?, ?, ?)
            ''',
            (entity_pk, material.parent_key, identifier, material.path_str))
    # TEXTURES
    for texture in description / "textures" // str:
        if isinstance(texture.data, str):
            identifier = texture.data
        else:
            identifier = None
        cursor.execute(
            '''
            INSERT INTO ClientEntityTextureField (
                ClientEntity_fk, shortName, identifier, jsonPath
            ) VALUES (?, ?, ?, ?)
            ''',
            (entity_pk, texture.parent_key, identifier, texture.path_str))
    # GEOMETRIES
    for geometry in description / "geometry" // str:
        if isinstance(geometry.data, str):
            identifier = geometry.data
        else:
            identifier = None
        cursor.execute(
            '''
            INSERT INTO ClientEntityGeometryField (
                ClientEntity_fk, shortName, identifier, jsonPath
            ) VALUES (?, ?, ?, ?)
            ''',
            (entity_pk, geometry.parent_key, identifier, geometry.path_str))

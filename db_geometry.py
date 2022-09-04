from sqlite3 import Connection
from pathlib import Path
from .better_json import load_jsonc


GEOMETRY_BUILD_SCRIPT = '''
-- Geometry
CREATE TABLE GeometryFile (
    GeometryFile_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    ResourcePack_fk INTEGER,

    path Path,
    FOREIGN KEY (ResourcePack_fk) REFERENCES ResourcePack (ResourcePack_pk)
        ON DELETE CASCADE
);
CREATE INDEX GeometryFile_ResourcePack_fk
ON GeometryFile (ResourcePack_fk);

CREATE TABLE Geometry (
    Geometry_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    GeometryFile_fk INTEGER NOT NULL,

    identifier TEXT,
    parent TEXT,
    jsonPath TEXT,

    FOREIGN KEY (GeometryFile_fk) REFERENCES GeometryFile (GeometryFile_pk)
        ON DELETE CASCADE
);
CREATE INDEX Geometry_GeometryFile_fk
ON Geometry (GeometryFile_fk);
'''

def load_geometries(db: Connection, rp_id: int):
    rp_path: Path = db.execute(
        "SELECT path FROM ResourcePack WHERE ResourcePack_pk = ?",
        (rp_id,)
    ).fetchone()[0]

    for geometry_path in (rp_path / "models").rglob("*.json"):
        load_geometry(db, geometry_path, rp_id)

def load_geometry(db: Connection, geometry_path: Path, rp_id: int):
    cursor = db.cursor()
    # GEOMETRY FILE
    cursor.execute(
        "INSERT INTO GeometryFile (path, ResourcePack_fk) VALUES (?, ?)",
        (geometry_path.as_posix(), rp_id)
    )
    file_pk = cursor.lastrowid
    geometry_jsonc = load_jsonc(geometry_path)
    # Try with 1.8.0 format
    for identifier in geometry_jsonc // str:
        if not identifier.parent_key.startswith("geometry."):
            continue
        # Check for inheritance
        split = identifier.parent_key.split(":", 1)
        id = identifier.parent_key
        parent = None
        if len(split)  == 2:
            id, parent = split
        cursor.execute(
            '''
            INSERT INTO Geometry (
                identifier, parent, GeometryFile_fk, jsonPath
            ) VALUES (?, ?, ?, ?)
            ''',
            (id, parent, file_pk, identifier.path_str)
        )
    # Try with 1.12.0 format
    geometries = (geometry_jsonc / "minecraft:geometry" // int)
    for geometry in geometries:
        identifier = geometry / 'description' / 'identifier'
        identifier_data = identifier.data
        if not isinstance(identifier_data, str):
            continue
        if not identifier_data.startswith("geometry."):
            continue
        cursor.execute(
            '''
            INSERT INTO Geometry (
                identifier, GeometryFile_fk, jsonPath
            ) VALUES (?, ?, ?)
            ''',
            (identifier_data, file_pk, geometry.path_str)
        )

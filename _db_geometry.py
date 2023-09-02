# pylint: disable=no-member, multiple-statements, missing-module-docstring, missing-class-docstring
from typing import cast, Optional
from sqlite3 import Connection
from pathlib import Path
import json
from .better_json_tools import load_jsonc
from ._views import dbtableview

@dbtableview(
    properties={
        "path": (Path, "NOT NULL")
    },
    connects_to=["ResourcePack"]
)
class GeometryFile: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "parent": (str, ""),
        "jsonPath": (str, "NOT NULL")
    },
    connects_to=["GeometryFile"]
)
class Geometry: ...

GEOMETRY_BUILD_SCRIPT: str = (
    GeometryFile.build_script +
    Geometry.build_script
)

def load_geometries(db: Connection, rp_id: int):
    '''
    Loads all geometries from the resource pack.
    '''
    rp_path: Path = db.execute(
        "SELECT path FROM ResourcePack WHERE ResourcePack_pk = ?",
        (rp_id,)
    ).fetchone()[0]

    for geometry_path in (rp_path / "models").rglob("*.json"):
        load_geometry(db, geometry_path, rp_id)

def load_geometry(db: Connection, geometry_path: Path, rp_id: int):
    '''
    Loads a geometry from the resource pack.
    '''
    cursor = db.cursor()
    # GEOMETRY FILE
    cursor.execute(
        "INSERT INTO GeometryFile (path, ResourcePack_fk) VALUES (?, ?)",
        (geometry_path.as_posix(), rp_id)
    )
    file_pk = cursor.lastrowid
    try:
        geometry_jsonc = load_jsonc(geometry_path)
    except json.JSONDecodeError:
        # sinlently skip invalid files. The file is in db but has no data
        return
    # Try with 1.8.0 format
    for identifier in geometry_jsonc // str:
        full_identifier = cast(str, identifier.parent_key)
        if not full_identifier.startswith("geometry."):
            continue
        # Check for inheritance
        split = full_identifier.split(":", 1)
        id_ = full_identifier
        parent: Optional[str] = None
        if len(split)  == 2:
            id_, parent = split
        cursor.execute(
            '''
            INSERT INTO Geometry (
                identifier, parent, GeometryFile_fk, jsonPath
            ) VALUES (?, ?, ?, ?)
            ''',
            (id_, parent, file_pk, identifier.path_str)
        )
    # Try with 1.12.0 format
    geometries = geometry_jsonc / "minecraft:geometry" // int
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

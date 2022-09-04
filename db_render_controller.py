from collections import defaultdict
from sqlite3 import Connection
from pathlib import Path
from typing import Literal, NamedTuple
from copy import copy

from .better_json import JSONWalker, load_jsonc
from .molang import find_molang_resources

RENDER_CONTROLLER_BUILD_SCRIPT = '''
-- Render controller
CREATE TABLE RenderControllerFile (
    RenderControllerFile_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    ResourcePack_fk INTEGER,

    path Path,
    FOREIGN KEY (ResourcePack_fk) REFERENCES ResourcePack (ResourcePack_pk)
        ON DELETE CASCADE
);
CREATE INDEX RenderControllerFile_ResourcePack_fk
ON RenderControllerFile (ResourcePack_fk);

CREATE TABLE RenderController (
    RenderController_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    RenderControllerFile_fk INTEGER NOT NULL,

    identifier TEXT,
    jsonPath TEXT,

    FOREIGN KEY (RenderControllerFile_fk) REFERENCES RenderControllerFile (RenderControllerFile_pk)
        ON DELETE CASCADE
);
CREATE INDEX RenderController_RenderControllerFile_fk
ON RenderController (RenderControllerFile_fk);

CREATE TABLE RenderControllerTexturesField (
    RenderControllerTexturesField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    RenderController_fk INTEGER NOT NULL,

    ownerArray TEXT,
    inOwnerArrayJsonPath TEXT, -- Path to the item in the owner array
    shortName TEXT,
    jsonPath TEXT,

    FOREIGN KEY (RenderController_fk) REFERENCES RenderController (RenderController_pk)
        ON DELETE CASCADE
);
CREATE INDEX RenderControllerTexturesField_RenderController_fk
ON RenderControllerTexturesField (RenderController_fk);

CREATE TABLE RenderControllerMaterialsField (
    RenderControllerMaterialsField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    RenderController_fk INTEGER NOT NULL,

    ownerArray TEXT,
    inOwnerArrayJsonPath TEXT, -- Path to the item in the owner array
    shortName TEXT,
    jsonPath TEXT,

    -- The star pattern that matches the bone name
    boneNamePattern TEXT,
    FOREIGN KEY (RenderController_fk) REFERENCES RenderController (RenderController_pk)
        ON DELETE CASCADE
);
CREATE INDEX RenderControllerMaterialsField_RenderController_fk
ON RenderControllerMaterialsField (RenderController_fk);

CREATE TABLE RenderControllerGeometryField (
    RenderControllerGeometryField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    RenderController_fk INTEGER NOT NULL,

    ownerArray TEXT,
    inOwnerArrayJsonPath TEXT, -- Path to the item in the owner array
    shortName TEXT,
    jsonPath TEXT,

    FOREIGN KEY (RenderController_fk) REFERENCES RenderController (RenderController_pk)
        ON DELETE CASCADE
);
CREATE INDEX RenderControllerGeometryField_RenderController_fk
ON RenderControllerGeometryField (RenderController_fk);
'''


def load_render_controllers(db: Connection, rp_id: int):
    rp_path: Path = db.execute(
        "SELECT path FROM ResourcePack WHERE ResourcePack_pk = ?",
        (rp_id,)
    ).fetchone()[0]

    for geometry_path in (rp_path / "render_controllers").rglob("*.json"):
        load_render_controller(db, geometry_path, rp_id)


class _LoadRcArraysItem(NamedTuple):
    '''
    One item on a list returned by _load_rc_arrays.
    '''
    short_name: str
    json_path: str

def _load_rc_arrays(
        rc: JSONWalker,
        array_type: Literal["geometry", "material", "texture"],
) -> dict[str, list[_LoadRcArraysItem]]:
    '''
    Loads arrays of specified type from a render controller.
    '''
    if array_type == "geometry":
        array_path = rc / "arrays" / "geometries" // str // int
    elif array_type == "material":
        array_path = rc / "arrays" / "materials" // str // int
    elif array_type == "texture":
        array_path = rc / "arrays" / "textures" // str // int
    else:
        raise ValueError(f"Invalid array type {array_type}")
    result = defaultdict(list)
    for obj in array_path:
        if not isinstance(obj.data, str):
            continue
        array_name = obj.parent.parent_key.lower()
        if not array_name.startswith("array."):
            continue
        array_name = array_name[6:]
        values = find_molang_resources(obj.data, [array_type])[array_type]
        path_str = obj.path_str
        result[array_name].extend(
            [_LoadRcArraysItem(v, path_str) for v in values])
    return dict(result)

def load_render_controller(db: Connection, entity_path: Path, rp_id: int):
    cursor = db.cursor()
    # RENDER CONTROLLER FILE
    cursor.execute(
        "INSERT INTO RenderControllerFile (path, ResourcePack_fk) VALUES (?, ?)",
        (entity_path.as_posix(), rp_id))
    file_pk = cursor.lastrowid
    entity_jsonc = load_jsonc(entity_path)
    for rc in entity_jsonc / 'render_controllers' // str:
        if not rc.parent_key.startswith("controller.render."):
            continue
        cursor.execute(
            '''
            INSERT INTO RenderController (
                identifier, RenderControllerFile_fk
            ) VALUES (?, ?)
            ''',
            (rc.parent_key, file_pk)
        )
        rc_pk = cursor.lastrowid
        # LOAD TEXTURES
        texture_arrays = _load_rc_arrays(rc, "texture")
        for field in rc / "textures" // int:
            if not isinstance(field.data, str):
                continue
            values = find_molang_resources(field.data, ["texture", "array"])
            # Direct access
            for texture_reference in values["texture"]:
                cursor.execute(
                    '''
                    INSERT INTO RenderControllerTexturesField (
                        RenderController_fk, shortName, jsonPath
                    ) VALUES (?, ?, ?)
                    ''',
                    (rc_pk, texture_reference, field.path_str)
                )
                values["texture"].remove(texture_reference)
            # Access through array
            for array_name in values["array"]:
                for texture_reference in texture_arrays.get(array_name, []):
                    cursor.execute(
                        '''
                        INSERT INTO RenderControllerTexturesField (
                            RenderController_fk,
                            shortName,
                            jsonPath,
                            ownerArray,
                            inOwnerArrayJsonPath
                        ) VALUES (?, ?, ?, ?, ?)
                        ''',
                        (
                            rc_pk,
                            texture_reference.short_name,
                            field.path_str,
                            array_name,
                            texture_reference.json_path
                        )
                    )
        # LOAD MATERIALS
        material_arrays = _load_rc_arrays(rc, "material")
        for field in rc / "materials" // int // str:
            if not isinstance(field.data, str):
                continue
            pattern = field.parent_key
            values = find_molang_resources(field.data, ["material", "array"])
            # Direct access
            for material_reference in copy(values["material"]):
                cursor.execute(
                    '''
                    INSERT INTO RenderControllerMaterialsField (
                        RenderController_fk, shortName, jsonPath,
                        boneNamePattern
                    ) VALUES (?, ?, ?, ?)
                    ''',
                    (rc_pk, material_reference, field.path_str, pattern)
                )
            # Access through array
            for array_name in values["array"]:
                for material_reference in material_arrays.get(array_name, []):
                    cursor.execute(
                        '''
                        INSERT INTO RenderControllerMaterialsField (
                            RenderController_fk,
                            shortName,
                            jsonPath,
                            ownerArray,
                            inOwnerArrayJsonPath,
                            boneNamePattern
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        ''',
                        (
                            rc_pk,
                            material_reference.short_name,
                            field.path_str,
                            array_name,
                            material_reference.json_path,
                            pattern
                        )
                    )
                    
        # LOAD GEOMETRIES
        geo_arrays = _load_rc_arrays(rc, "geometry")
        field = rc / "geometry"
        if isinstance(field.data, str):
            values = find_molang_resources(field.data, ["geometry", "array"])
            # Direct access
            for geometry_reference in copy(values["geometry"]):
                cursor.execute(
                    '''
                    INSERT INTO RenderControllerGeometryField (
                        RenderController_fk, shortName, jsonPath
                    ) VALUES (?, ?, ?)
                    ''',
                    (rc_pk, geometry_reference, field.path_str)
                )
            # Access through array
            for array_name in values["array"]:
                for geometry_reference in geo_arrays.get(array_name, []):
                    cursor.execute(
                        '''
                        INSERT INTO RenderControllerGeometryField (
                            RenderController_fk,
                            shortName,
                            jsonPath,
                            ownerArray,
                            inOwnerArrayJsonPath
                        ) VALUES (?, ?, ?, ?, ?)
                        ''',
                        (
                            rc_pk,
                            geometry_reference.short_name,
                            field.path_str,
                            array_name,
                            geometry_reference.json_path
                        )
                    )

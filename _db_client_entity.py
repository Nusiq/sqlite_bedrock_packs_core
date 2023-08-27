from sqlite3 import Connection
from pathlib import Path
from .better_json_tools import load_jsonc
import json
from ._views import dbtableview, WeakTableConnection

@dbtableview(
    properties={
        "path": (Path, "NOT NULL")
    },
    connects_to=["ResourcePack"]
)
class ClientEntityFile: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL")
    },
    connects_to=["ClientEntityFile"],
    weak_connects_to=[
        WeakTableConnection("identifier", "Entity", "identifier")
    ]
)
class ClientEntity: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "condition": (str, ""),
        "jsonPath": (str, "NOT NULL")
    },
    connects_to=["ClientEntity"],
    weak_connects_to=[
        WeakTableConnection("identifier", "RenderController", "identifier")
    ]
)
class ClientEntityRenderControllerField: ...

@dbtableview(
    properties={
        "shortName": (str, "NOT NULL"),
        "identifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL")
    },
    connects_to=["ClientEntity"],
    weak_connects_to=[
        WeakTableConnection("identifier", "Geometry", "identifier")
    ]
)
class ClientEntityGeometryField: ...

@dbtableview(
    properties={
        "shortName": (str, "NOT NULL"),

        # identifier is the path without the extension
        "identifier": (str, "NOT NULL"),

        "jsonPath": (str, "NOT NULL")
    },
    connects_to=["ClientEntity"],
    weak_connects_to=[
        WeakTableConnection("identifier", "TextureFile", "identifier")
    ]
)
class ClientEntityTextureField: ...

@dbtableview(
    properties={
        "shortName": (str, "NOT NULL"),
        "identifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL")
    },
    connects_to=["ClientEntity"]
)
class ClientEntityMaterialField: ...

@dbtableview(
    properties={
        "shortName": (str, "NOT NULL"),
        "identifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL")
    },
    connects_to=["ClientEntity"],
    weak_connects_to=[
        WeakTableConnection("identifier", "RpAnimation", "identifier")
    ]
)
class ClientEntityAnimationField: ...

@dbtableview(
    properties={
        "shortName": (str, "NOT NULL"),
        "identifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL")
    },
    connects_to=["ClientEntity"],
    weak_connects_to=[
        WeakTableConnection("identifier", "RpAnimationController", "identifier")
    ]
)
class ClientEntityAnimationControllerField: ...

CLIENT_ENTITY_BUILD_SCRIPT: str = (
    ClientEntityFile.build_script +
    ClientEntity.build_script +
    ClientEntityRenderControllerField.build_script +
    ClientEntityGeometryField.build_script +
    ClientEntityTextureField.build_script +
    ClientEntityMaterialField.build_script +
    ClientEntityAnimationField.build_script +
    ClientEntityAnimationControllerField.build_script
)

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
        return  # Skip entitites without identifier
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
            continue  #  identifier must be NOT null
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
            continue  # identifier must be NOT null
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
            continue  # identifier must be NOT null
        cursor.execute(
            '''
            INSERT INTO ClientEntityGeometryField (
                ClientEntity_fk, shortName, identifier, jsonPath
            ) VALUES (?, ?, ?, ?)
            ''',
            (entity_pk, geometry.parent_key, identifier, geometry.path_str))
    # ANIMATIONS & ANIMATION CONTROLLERS
    for animation in description / "animations" // str:
        if isinstance(animation.data, str):
            identifier = animation.data
        else:
            continue
        if animation.data.startswith("controller.animation."):
            # Animation Controllers
            cursor.execute(
                '''
                INSERT INTO ClientEntityAnimationControllerField (
                    ClientEntity_fk, shortName, identifier, jsonPath
                ) VALUES (?, ?, ?, ?)
                ''',
                (entity_pk, animation.parent_key, identifier, animation.path_str))
        elif animation.data.startswith("animation."):
            # Animations
            cursor.execute(
                '''
                INSERT INTO ClientEntityAnimationField (
                    ClientEntity_fk, shortName, identifier, jsonPath
                ) VALUES (?, ?, ?, ?)
                ''',
                (entity_pk, animation.parent_key, identifier, animation.path_str))

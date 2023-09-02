# pylint: disable=no-member, multiple-statements, missing-module-docstring, missing-class-docstring
from typing import cast
from sqlite3 import Connection
from pathlib import Path
import json
from .better_json_tools import load_jsonc
from ._views import dbtableview, WeakTableConnection

@dbtableview(
    properties={
        "path": (Path, "NOT NULL")
    },
    connects_to=["BehaviorPack"]
)
class BpBlockFile: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
    },
    connects_to=["BpBlockFile"]
)
class BpBlock: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL"),
    },
    connects_to=["BpBlock"],
    weak_connects_to=[
        WeakTableConnection("identifier", "LootTable", "identifier")
    ]
)
class BpBlockLootField: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL"),
    },
    connects_to=["BpBlock"],
    weak_connects_to=[
        WeakTableConnection("identifier", "Geometry", "identifier")
    ]
)
class BpBlockGeometryField: ...

@dbtableview(
    properties={
        "jsonPath": (str, "NOT NULL"),
    },
    connects_to=["BpBlock"]
)
class BpBlockMaterialInstancesField: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL"),
        "texture": (str, "NOT NULL"),
        "renderMethod": (str, "NOT NULL"),
    },
    connects_to=["BpBlockMaterialInstancesField"],
    weak_connects_to=[
        WeakTableConnection("texture", "TerrainTexture", "identifier")
    ]
)
class BpBlockMaterialInstancesFieldInstance: ...

BP_BLOCK_BUILD_SCRIPT: str = (
    BpBlockFile.build_script +
    BpBlock.build_script +
    BpBlockLootField.build_script +
    BpBlockGeometryField.build_script +
    BpBlockMaterialInstancesField.build_script +
    BpBlockMaterialInstancesFieldInstance.build_script
)


def load_bp_blocks(db: Connection, bp_id: int):
    '''
    Loads all blocks from the behavior pack.
    '''
    bp_path: Path = db.execute(
        "SELECT path FROM BehaviorPack WHERE BehaviorPack_pk = ?",
        (bp_id,)
    ).fetchone()[0]
    for bp_block_path in (bp_path / "blocks").rglob("*.json"):
        load_bp_block(db, bp_block_path, bp_id)

def load_bp_block(db: Connection, bp_block_path: Path, bp_id: int):
    '''
    Loads a block from the behavior pack.
    '''
    cursor = db.cursor()
    # BP BLOCK FILE
    cursor.execute(
        "INSERT INTO BpBlockFile (path, BehaviorPack_fk) VALUES (?, ?)",
        (bp_block_path.as_posix(), bp_id)
    )
    file_pk = cursor.lastrowid
    # BP BLOCK
    try:
        bp_block_json = load_jsonc(bp_block_path)
    except json.JSONDecodeError:
        return  # Skip silently

    block_walker = (
        bp_block_json / "minecraft:block")
    block_identifier = (block_walker / "description" / "identifier").data
    if not isinstance(block_identifier, str):
        return  # Skip blocks without identifier
    cursor.execute(
        '''
        INSERT INTO BpBlock (
        identifier, BpBlockFile_fk
        ) VALUES (?, ?)
        ''',
        (block_identifier, file_pk))
    bp_block_pk = cursor.lastrowid

    # BP BLOCK LOOT FIELD
    all_components_walker = (
        block_walker / "permutations" // int / "components" +
        block_walker / "components"
    )
    for loot_walker in all_components_walker / "minecraft:loot":
        loot = loot_walker.data
        if not isinstance(loot, str):
            continue
        cursor.execute(
            '''
            INSERT INTO BpBlockLootField (
            identifier, jsonPath, BpBlock_fk
            ) VALUES (?, ?, ?)
            ''',
            (loot, loot_walker.path_str, bp_block_pk))
    # BP BLOCK GEOMETRY FIELD
    for geometry_walker in all_components_walker / "minecraft:geometry":
        geometry = geometry_walker.data
        if not isinstance(geometry, str):
            continue
        cursor.execute(
            '''
            INSERT INTO BpBlockGeometryField (
            identifier, jsonPath, BpBlock_fk
            ) VALUES (?, ?, ?)
            ''',
            (geometry, geometry_walker.path_str, bp_block_pk))
    # BP BLOCK MATERIAL INSTANCES FIELD
    for material_instances_walker in all_components_walker / "minecraft:material_instances":
        material_instance = material_instances_walker.data
        if not isinstance(material_instance, dict):
            continue
        cursor.execute(
            '''
            INSERT INTO BpBlockMaterialInstancesField (
            jsonPath, BpBlock_fk
            ) VALUES (?, ?)
            ''',
            (material_instances_walker.path_str, bp_block_pk))
        material_instances_pk = cursor.lastrowid
        # BP BLOCK MATERIAL INSTANCES FIELD INSTANCE
        for instance_walker in material_instances_walker // str:
            instance_identifier = instance_walker.parent_key
            instance_identifier = cast(str, instance_identifier)
            instance_texture = (instance_walker / "texture").data
            if not isinstance(instance_texture, str):
                continue
            instance_render_method = (instance_walker / "render_method").data
            if not isinstance(instance_render_method, str):
                continue
            cursor.execute(
                '''
                INSERT INTO BpBlockMaterialInstancesFieldInstance (
                identifier, jsonPath, texture, renderMethod, BpBlockMaterialInstancesField_fk
                ) VALUES (?, ?, ?, ?, ?)
                ''',
                (
                    instance_identifier,
                    instance_walker.path_str,
                    instance_texture,
                    instance_render_method,
                    material_instances_pk
                ))

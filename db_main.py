from pathlib import Path
from sqlite3 import Connection
import sqlite3
from typing import Collection, Literal

from .db_resource_pack import RESOURCE_PACK_BUILD_SCRIPT, load_resource_pack
from .db_geometry import GEOMETRY_BUILD_SCRIPT, load_geometries
from .db_client_entity import CLIENT_ENTITY_BUILD_SCRIPT, load_client_entities
from .db_render_controller import (
    RENDER_CONTROLLER_BUILD_SCRIPT, load_render_controllers)
from .db_texture import TEXTURE_BUILD_SCRIPT, load_textures
from .db_particle import PARTICLE_BUILD_SCRIPT, load_particles
from .db_rp_animation import RP_ANIMATION_BUILD_SCRIPT, load_rp_animations
from .db_rp_animation_controller import (
    RP_ANIMATION_CONTROLLER_BUILD_SCRIPT, load_rp_animation_controllers)
from .db_attachable import ATTACHABLE_BUILD_SCRIPT, load_attachables

SCRIPT = '''
PRAGMA foreign_keys = ON;
'''

def _path_adapter(path: Path):
    return path.as_posix()


def _path_converter(path: bytes):
    return Path(path.decode('utf8'))

def create_db(db_path: str = ":memory:") -> Connection:
    '''
    Creates a new dtabase at db_path. Runs all of the build scripts of all of
    the database components.
    '''
    sqlite3.register_adapter(Path, _path_adapter)
    sqlite3.register_converter("Path", _path_converter)
    db = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    db.row_factory = sqlite3.Row

    db.executescript(SCRIPT)
    db.executescript(RESOURCE_PACK_BUILD_SCRIPT)

    db.executescript(CLIENT_ENTITY_BUILD_SCRIPT)
    db.executescript(RENDER_CONTROLLER_BUILD_SCRIPT)
    db.executescript(GEOMETRY_BUILD_SCRIPT)
    db.executescript(TEXTURE_BUILD_SCRIPT)
    db.executescript(PARTICLE_BUILD_SCRIPT)
    db.executescript(RP_ANIMATION_BUILD_SCRIPT)
    db.executescript(RP_ANIMATION_CONTROLLER_BUILD_SCRIPT)
    db.executescript(ATTACHABLE_BUILD_SCRIPT)
    return db

def open_db(db_path: str) -> Connection:
    '''
    Opens a database previously created by sqlite_bedrock_packs. This function
    doesn't check if the database has a valid structure. It assumes it does.
    The only thing it does except opening the database is to register the
    Path type adapter and converter.
    '''
    sqlite3.register_adapter(Path, _path_adapter)
    sqlite3.register_converter("Path", _path_converter)
    return sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)

DbItems = Literal[
    "geometries",
    "client_entities",
    "render_controllers",
    "textures",
    "particles",
    "rp_animations",
    "rp_animation_controllers",
    "attachables",
]

def load_rp(
        db: Connection,
        rp_path: Path, *,
        include: Collection[DbItems] = (
            "geometries",
            "client_entities",
            "render_controllers",
            "textures",
            "particles",
            "rp_animations",
            "rp_animation_controllers",
            "attachables",
        ),
        exclude: Collection[DbItems] = tuple()
    ) -> None:

    rp_pk = load_resource_pack(db, rp_path)
    if (
            "geometries" in include and
            "geometries" not in exclude):
        load_geometries(db, rp_pk)
    if (
            "client_entities" in include and
            "client_entities" not in exclude):
        load_client_entities(db, rp_pk)
    if (
            "render_controllers" in include and
            "render_controllers" not in exclude):
        load_render_controllers(db, rp_pk)
    if (
            "textures" in include and
            "textures" not in exclude):
        load_textures(db, rp_pk)
    if (
            "particles" in include and
            "particles"  not in exclude):
        load_particles(db, rp_pk)
    if (
            "rp_animations " in include and
            "rp_animations"  not in exclude):
        load_rp_animations(db, rp_pk)
    if (
            "rp_animation_controllers " in include and
            "rp_animation_controllers"  not in exclude):
        load_rp_animation_controllers(db, rp_pk)
    if (
            "attachables " in include and
            "attachables"  not in exclude):
        load_attachables(db, rp_pk)
    db.commit()

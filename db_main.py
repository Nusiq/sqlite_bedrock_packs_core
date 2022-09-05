from pathlib import Path
from sqlite3 import Connection
import sqlite3

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

def load_rp(
        db: Connection, rp_path: Path, geometries=True, client_entities=True,
        render_controllers=True, textures=True, particles=True,
        rp_animations=True, rp_animation_controllers=True, attachables=True):
    rp_pk = load_resource_pack(db, rp_path)
    if geometries:
        load_geometries(db, rp_pk)
    if client_entities:
        load_client_entities(db, rp_pk)
    if render_controllers:
        load_render_controllers(db, rp_pk)
    if textures:
        load_textures(db, rp_pk)
    if particles:
        load_particles(db, rp_pk)
    if rp_animations:
        load_rp_animations(db, rp_pk)
    if rp_animation_controllers:
        load_rp_animation_controllers(db, rp_pk)
    if attachables:
        load_attachables(db, rp_pk)

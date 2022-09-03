from pathlib import Path
from sqlite3 import Connection
import sqlite3

from .db_resource_pack import RESOURCE_PACK_BUILD_SCRIPT, load_resource_pack
from .db_geometry import GEOMETRY_BUILD_SCRIPT, load_geometries
from .db_client_entity import CLIENT_ENTITY_BUILD_SCRIPT, load_client_entities
from .db_render_controller import RENDER_CONTROLLER_BUILD_SCRIPT, load_render_controllers
from .db_texture import TEXTURE_BUILD_SCRIPT, load_textures

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
    return db

def load_rp(
        db: Connection, rp_path: Path, geometries=True, client_entities=True,
        render_controllers=True, textures=True):
    rp_pk = load_resource_pack(db, rp_path)
    if geometries:
        load_geometries(db, rp_pk)
    if client_entities:
        load_client_entities(db, rp_pk)
    if render_controllers:
        load_render_controllers(db, rp_pk)
    if textures:
        load_textures(db, rp_pk)
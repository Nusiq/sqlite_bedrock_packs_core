from pathlib import Path
from sqlite3 import Connection
import sqlite3
from typing import Iterable, Literal

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
from .db_sound_definitions import (
    SOUND_DEFINITIONS_BUILD_SCRIPT, load_sound_definitions)
from .db_sound import (
    SOUND_BUILD_SCRIPT, load_sounds)
from .db_behavior_pack import (
    BEHAVIOR_PACK_BUILD_SCRIPT, load_behavior_pack)


SCRIPT = '''
PRAGMA foreign_keys = ON;
'''

def _path_adapter(path: Path):
    return path.as_posix()


def _path_converter(path: bytes):
    return Path(path.decode('utf8'))

def create_db(db_path: str = ":memory:") -> Connection:
    '''
    Creates a new dtabase in :code:`db_path`. Runs all of the build scripts of
    the database components.

    :param db_path: The path to the database file. The argument is passed to
        :func:`sqlite3.connect` If the argument is :code:`":memory:"`, the
        database is created in memory. :code:`":memory:"` is the default value.
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
    db.executescript(SOUND_DEFINITIONS_BUILD_SCRIPT)
    db.executescript(SOUND_BUILD_SCRIPT)

    db.executescript(BEHAVIOR_PACK_BUILD_SCRIPT)
    return db

def open_db(db_path: str) -> Connection:
    '''
    Opens a database file. Usually these files are created by
    :func:`create_db`. This function doesn't check if the database has a valid
    structure. It assumes it does. This function only opens the database and
    sets some sqlite3 adapter and converter functions for Path objects.

    :param db_path: The path to the database file. The argument is passed to
        :func:`sqlite3.connect`.
    '''
    sqlite3.register_adapter(Path, _path_adapter)
    sqlite3.register_converter("Path", _path_converter)
    return sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)

DbRpItems = Literal[
    "geometries",
    "client_entities",
    "render_controllers",
    "textures",
    "particles",
    "rp_animations",
    "rp_animation_controllers",
    "attachables",
    "sound_definitions",
    "sounds",
]
'''
Possible values of :code:`include and :code:`exclude` arguments of
:func:`load_rp` function.
'''

def load_rp(
        db: Connection,
        rp_path: Path, *,
        include: Iterable[DbRpItems] = (
            "geometries",
            "client_entities",
            "render_controllers",
            "textures",
            "particles",
            "rp_animations",
            "rp_animation_controllers",
            "attachables",
            "sound_definitions",
            "sounds",
        ),
        exclude: Iterable[DbRpItems] = tuple()
    ) -> None:
    '''
    Loads resource pack data into the database.

    :param db: The database connection.
    :param rp_path: The path to the resource pack.
    :param include: A list of items to include. By default, all items are
        included.
    :param exclude: A list of items to exclude. By default, no items are
        excluded.

    If there is an item in both include and exclude, it is excluded. The
    include and exclude lists accept strings that are the names of the
    supported database components.
    '''
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
            "rp_animations" in include and
            "rp_animations"  not in exclude):
        load_rp_animations(db, rp_pk)
    if (
            "rp_animation_controllers" in include and
            "rp_animation_controllers"  not in exclude):
        load_rp_animation_controllers(db, rp_pk)
    if (
            "attachables" in include and
            "attachables"  not in exclude):
        load_attachables(db, rp_pk)
    if (
            "sound_definitions" in include and
            "sound_definitions"  not in exclude):
        load_sound_definitions(db, rp_pk)
    if (
            "sounds" in include and
            "sounds"  not in exclude):
        load_sounds(db, rp_pk)
    db.commit()

def load_bp(db: Connection, bp_path: Path) -> None:
    '''
    Loads behavior pack data into the database. Currently, this function only
    loads the Behavior Pack table and doesn't load any objects from the pack.

    :param db: The database connection.
    :param rp_path: The path to the resource pack.
    '''
    load_behavior_pack(db, bp_path)
    db.commit()

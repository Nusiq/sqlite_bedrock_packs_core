from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from sqlite3 import Connection
import sqlite3
from typing import Literal, Union
from collections.abc import Container

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
from .db_rp_item import (
    RP_ITEM_BUILD_SCRIPT, load_rp_items)
from .db_behavior_pack import (
    BEHAVIOR_PACK_BUILD_SCRIPT, load_behavior_pack)
from .db_entity import (
    ENTITY_BUILD_SCRIPT, load_entities)
from .db_loot_table import (
    LOOT_TABLE_BUILD_SCRIPT, load_loot_tables)
from .db_trade_table import (
    TRADE_TABLE_BUILD_SCRIPT, load_trade_tables)
from .db_bp_animation import (
    BP_ANIMATION_BUILD_SCRIPT, load_bp_animations)
from .db_bp_animation_controller import (
    BP_ANIMATION_CONTROLLER_BUILD_SCRIPT, load_bp_animation_controllers)
from .db_bp_item import (
    BP_ITEM_BUILD_SCRIPT, load_bp_items)

# SQLite3 converters/adapters
def _path_adapter(path: Path):
    return path.as_posix()

def _path_converter(path: bytes):
    return Path(path.decode('utf8'))

# Types for include/exclude arguments of loading RP/BP
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
    "rp_items",
]

DbBpItems = Literal[
    "entities",
    "loot_tables",
    "trade_tables",
    "bp_animations",
    "bp_animation_controllers",
    "bp_items"
]

# The main database class
@dataclass
class Database:
    '''
    A class that represents a database with resource packs and behavior packs.
    '''

    connection: Connection
    '''The SQLite database conncetion.'''

    @staticmethod
    def open(db_path: Union[str, Path]) -> Database:
        '''
        Creates a database using  path to the database file.
        This function doesn't check if the database has a valid structure. It
        assumes it does. This function only opens the database and
        sets some sqlite3 adapter and converter functions for Path objects.

        :param db_path: the path to the database file
        '''
        if isinstance(db_path, Path):
            db_path = db_path.as_posix()


        sqlite3.register_adapter(Path, _path_adapter)
        sqlite3.register_converter("Path", _path_converter)
        db = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)

        return Database(db)

    @staticmethod
    def create(db_path: Union[str, Path] = ":memory:") -> Database:
        '''
        Creates a new database for storing resource packs and behavior packs in
        memory or in a file. The default value is :code:`":memory:"` which
        means that the database is created in memory (just like in sqlite3).

        :param db_path: The path to the database file or :code:`":memory:"`.
        '''
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

        db.executescript("PRAGMA foreign_keys = ON;\n")

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
        db.executescript(RP_ITEM_BUILD_SCRIPT)

        db.executescript(BEHAVIOR_PACK_BUILD_SCRIPT)
        db.executescript(ENTITY_BUILD_SCRIPT)
        db.executescript(LOOT_TABLE_BUILD_SCRIPT)
        db.executescript(TRADE_TABLE_BUILD_SCRIPT)
        db.executescript(BP_ANIMATION_BUILD_SCRIPT)
        db.executescript(BP_ANIMATION_CONTROLLER_BUILD_SCRIPT)
        db.executescript(BP_ITEM_BUILD_SCRIPT)

        return Database(db)

    def load_rp(
        self,
        rp_path: Path, *,
        include: Container[DbRpItems] = (
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
            "rp_items"
        ),
        exclude: Container[DbRpItems] = tuple()
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
        rp_pk = load_resource_pack(self.connection, rp_path)
        if (
                "geometries" in include and
                "geometries" not in exclude):
            load_geometries(self.connection, rp_pk)
        if (
                "client_entities" in include and
                "client_entities" not in exclude):
            load_client_entities(self.connection, rp_pk)
        if (
                "render_controllers" in include and
                "render_controllers" not in exclude):
            load_render_controllers(self.connection, rp_pk)
        if (
                "textures" in include and
                "textures" not in exclude):
            load_textures(self.connection, rp_pk)
        if (
                "particles" in include and
                "particles" not in exclude):
            load_particles(self.connection, rp_pk)
        if (
                "rp_animations" in include and
                "rp_animations" not in exclude):
            load_rp_animations(self.connection, rp_pk)
        if (
                "rp_animation_controllers" in include and
                "rp_animation_controllers" not in exclude):
            load_rp_animation_controllers(self.connection, rp_pk)
        if (
                "attachables" in include and
                "attachables" not in exclude):
            load_attachables(self.connection, rp_pk)
        if (
                "sound_definitions" in include and
                "sound_definitions" not in exclude):
            load_sound_definitions(self.connection, rp_pk)
        if (
                "sounds" in include and
                "sounds" not in exclude):
            load_sounds(self.connection, rp_pk)
        if (
                "rp_items" in include and
                "rp_items" not in exclude):
            load_rp_items(self.connection, rp_pk)
        self.connection.commit()

    def load_bp(
            self,
            bp_path: Path, *,
            include: Container[DbBpItems] = (
                "entities",
                "loot_tables",
                "trade_tables",
                "bp_animations",
                "bp_animation_controllers",
                "bp_items",
            ),
            exclude: Container[DbBpItems] = tuple()) -> None:
        '''
        Loads behavior pack data into the database.

        :param db: The database connection.
        :param bp_path: The path to the resource pack.
        :param include: A list of items to include. By default, all items are
            included.
        :param exclude: A list of items to exclude. By default, no items are
            excluded.

        If there is an item in both include and exclude, it is excluded. The
        include and exclude lists accept strings that are the names of the
        supported database components.
        '''
        bp_pk = load_behavior_pack(self.connection, bp_path)
        if (
                "entities" in include and
                "entities" not in exclude):
            load_entities(self.connection, bp_pk)
        if (
                "loot_tables" in include and
                "loot_tables" not in exclude):
            load_loot_tables(self.connection, bp_pk)
        if (
                "trade_tables" in include and
                "trade_tables" not in exclude):
            load_trade_tables(self.connection, bp_pk)
        if (
                "bp_animations" in include and
                "bp_animations"  not in exclude):
            load_bp_animations(self.connection, bp_pk)
        if (
                "bp_animation_controllers" in include and
                "bp_animation_controllers"  not in exclude):
            load_bp_animation_controllers(self.connection, bp_pk)
        if (
                "bp_items" in include and
                "bp_items"  not in exclude):
            load_bp_items(self.connection, bp_pk)
        self.connection.commit()

    def close(self):
        '''
        Runs close() function on the database connection.
        '''
        self.connection.close()
        
    def commit(self):
        '''
        Runs commit() function on the database connection.
        '''
        self.connection.commit()

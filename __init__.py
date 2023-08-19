from __future__ import annotations

import sqlite3
from collections import deque
from collections.abc import Container
from dataclasses import dataclass
from pathlib import Path
from sqlite3 import Connection
from typing import (
    Iterable, Iterator, Literal, NamedTuple, Optional, Union,
    TypeVar, overload, TYPE_CHECKING, Any, Type)

# The BP and RP must be imported before other _db_* modules because they are
# roots of the dependency graph.
from .views import *
from ._views import (
    RELATION_MAP, WRAPPER_CLASSES, add_reverse_connections,
    validate_weak_connections, AbstractDBView)

VERSION = (3, 0, 2)  # COMPATIBILITY BREAK, NEW FEATURE, BUGFIX
__version__ = '.'.join([str(x) for x in VERSION])

# SQLite3 converters/adapters
def _path_adapter(path: Path):
    return path.as_posix()

def _path_converter(path: bytes):
    return Path(path.decode('utf8'))


# TYPES FOR INCLUDE/EXCLUDE ARGUMENTS OF LOADING RP/BP
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

# THE MAIN DATABASE CLASS
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
                "bp_animations" not in exclude):
            load_bp_animations(self.connection, bp_pk)
        if (
                "bp_animation_controllers" in include and
                "bp_animation_controllers" not in exclude):
            load_bp_animation_controllers(self.connection, bp_pk)
        if (
                "bp_items" in include and
                "bp_items" not in exclude):
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


_T = TypeVar("_T", bound=AbstractDBView)
_T2 = TypeVar("_T2", bound=AbstractDBView)
_T3 = TypeVar("_T3", bound=AbstractDBView)
_T4 = TypeVar("_T4", bound=AbstractDBView)
_T5 = TypeVar("_T5", bound=AbstractDBView)
_T6 = TypeVar("_T6", bound=AbstractDBView)
_T7 = TypeVar("_T7", bound=AbstractDBView)
_T8 = TypeVar("_T8", bound=AbstractDBView)
_T9 = TypeVar("_T9", bound=AbstractDBView)

class Left(NamedTuple):
    '''
    A helper class to indicate that a table should be joined using LEFT join in
    the query of :class:`EasyQuery` class.
    '''
    value: str
    '''The name of the table'''

if TYPE_CHECKING:
    # This is a hack to satisfy the type checker. IT makes it think that
    # Left is a function that returns the same type as the argument which
    # is later used in the EasyQuery class to deterimine the types of the
    # arguments to yield.
    def Left(value: Type[_T]) -> Type[_T]:
        return value

def build_easy_query(
        root: _T2,
        *tables: Any,
        blacklist: Iterable[str] = ("BehaviorPack", "ResourcePack"),
        accept_non_pk: bool = True,
        distinct: bool = True,
        where: Optional[list[str]] = None,
        group_by: Optional[list[str]] = None,
        having: Optional[list[str]] = None,
        order_by: Optional[list[str]] = None) -> str:
    '''
    Returns a string with a SQL query that can be used to query the
    sqlite_bedrock_packs database. The query is built using the class names
    of the wrapper classes for the tables or their names wrapped in the
    Left object in case of LEFT join. The results of the query are the primary
    keys of the selected tables.

    Example:

    .. code-block:: python

        >>> query = build_easy_query(
        ...     Entity, Left(Geometry), RpAnimation,
        ...     accept_non_pk=True,
        ...     where=[
        ...         "EntityFile.EntityFile_pk == 1"
        ...     ]
        ... ).sql_code
        >>> print(query)
        SELECT DISTINCT
                Entity_pk AS Entity,
                Geometry_pk AS Geometry,
                RpAnimation_pk AS RpAnimation
        FROM Entity
        JOIN ClientEntity
                ON Entity.identifier = ClientEntity.identifier
        JOIN ClientEntityGeometryField
                ON ClientEntity.ClientEntity_pk = ClientEntityGeometryField.ClientEntity_fk
        LEFT JOIN Geometry
                ON ClientEntityGeometryField.identifier = Geometry.identifier
        JOIN ClientEntityAnimationField
                ON ClientEntity.ClientEntity_pk = ClientEntityAnimationField.ClientEntity_fk
        JOIN RpAnimation
                ON ClientEntityAnimationField.identifier = RpAnimation.identifier
        WHERE
                EntityFile.EntityFile_pk == 1

    :param root: The root table to start the query from.
    :param tables: A list of tables to join. Use Left to indicate that a table
        should be joined using LEFT join. The tables don't need to have a
        direct connection between each other, the connections will be found
        automatically if necessary relations exist (this means that the query
        genrated can include tables that are not in the list).
    :param blacklist: A list of tables to ignore while searching for
        connections. By default it's BehaviorPack and ResourcePack because
        otherwise in many cases the query would look for objects from the same
        packs instead of using different more useful connections.
    :param accept_non_pk: Whether to accept non-primary key relations.
        By default it's True. The primary key connections only cover the
        situations where it's guaranteed that the relation is valid (like
        a relation between EntityFile and Entity). Most of the connections
        that might be useful to query are non-primary key connections (like
        a relation between a ClientEntity and RenderController).
    :param distinct: Whether to use DISTINCT in the query. By default it's
        True. It's recommended to use it when querying for multiple tables
        because otherwise the query might return the same row multiple times.
    :param where: A list of constraints to add to the query. This is a
        list of strings with raw SQL code which is inserted into the WHERE
        part of the query. The constraints are joined using AND.
    :param group_by: A list of columns to group the results by. This is a
        list of strings with raw SQL code which is inserted into the GROUP BY
        part of the query.
    :param having: A list of constraints to add to the query. This is a
        list of strings with raw SQL code which is inserted into the HAVING
        part of the query. The constraints are joined using AND.
    :param order_by: A list of columns to order the results by. This is a
        list of strings with raw SQL code which is inserted into the ORDER BY
        part of the query.
    '''
    all_tables: list[Union[_T, Left]] = [root, *tables]
    for t in all_tables:
        t_name = t.value.__name__ if isinstance(t, Left) else t.__name__
        if t_name not in RELATION_MAP.keys():
            raise ValueError(
                f"Table '{t_name}' does not exist in the database.")
    prev_t = None
    joined_connections: list[_EasyQueryConnection] = []
    blacklist = list(blacklist)
    for t in all_tables:
        if prev_t is None:
            prev_t = t
            continue
        left = False
        if isinstance(t, Left):
            left = True
            t = t.value
        connection = _find_connection(
            prev_t.__name__, t.__name__, accept_non_pk, set(blacklist))
        if connection is None:
            raise ValueError(
                f"No connection between {prev_t.__name__} and {t.__name__} "
                f"after excluding tables: {', '.join(blacklist)}")
        # if left and len(connection) > 1:
        if left:
            connection[-1].left_join = True
        joined_connections.extend(connection)
        prev_t = t
    # Strip joined_connections of duplicates
    reduced_joined_connections = []
    if len(joined_connections) > 0:
        known_connections = {joined_connections[0].left}
        for jc in joined_connections:
            if jc.right in known_connections:
                continue
            known_connections.add(jc.right)
            reduced_joined_connections.append(jc)

    # Convert all tables to list[str] (we don't need Left objects anymore)
    # the infromation is in the reduced_joined_queries
    all_tables_str = [
        t.value.__name__
        if isinstance(t, Left)
        else t.__name__
        for t in all_tables]
    # Build the quer
    selection = ",\n\t".join([f"{t}_pk AS {t}" for t in all_tables_str])
    select = "SELECT DISTINCT" if distinct else "SELECT"
    query = f'{select}\n\t{selection}\nFROM {all_tables_str[0]}'
    for c in reduced_joined_connections:
        join = "LEFT JOIN" if c.left_join else "JOIN"
        query += (
            f'\n{join} {c.right}\n'
            f'\tON {c.left}.{c.left_column} = {c.right}.{c.right_column}')
    if where is not None:
        if isinstance(where, str):
            where = [where]
        query += f'\nWHERE\n\t'+"\n\tAND ".join(where)
    if group_by is not None:
        if isinstance(group_by, str):
            group_by = [group_by]
        query += f'\nGROUP BY\n\t'+"\n\t, ".join(group_by)
    if having is not None:
        if isinstance(having, str):
            having = [having]
        query += f'\nHAVING\n\t'+"\n\tAND ".join(having)
    if order_by is not None:
        if isinstance(order_by, str):
            order_by = [order_by]
        query += f'\nORDER BY\n\t'+"\n\t, ".join(order_by)
    return query

@overload
def yield_from_easy_query(
        db: Union[Connection, Database],
        t1: Type[_T],
        /,
        blacklist: Iterable[str] = ...,
        accept_non_pk: bool = ...,
        distinct: bool = ...,
        where: Optional[list[str]] = ...,
        group_by: Optional[list[str]] = ...,
        having: Optional[list[str]] = ...,
        order_by: Optional[list[str]] = ...
) -> Iterator[tuple[_T]]: ...
@overload
def yield_from_easy_query(
        db: Union[Connection, Database],
        t1: Type[_T],
        t2: Type[_T2],
        /,
        blacklist: Iterable[str] = ...,
        accept_non_pk: bool = ...,
        distinct: bool = ...,
        where: Optional[list[str]] = ...,
        group_by: Optional[list[str]] = ...,
        having: Optional[list[str]] = ...,
        order_by: Optional[list[str]] = ...
) -> Iterator[tuple[_T, _T2]]: ...
@overload
def yield_from_easy_query(
        db: Union[Connection, Database],
        t1: Type[_T],
        t2: Type[_T2],
        t3: Type[_T3],
        /,
        blacklist: Iterable[str] = ...,
        accept_non_pk: bool = ...,
        distinct: bool = ...,
        where: Optional[list[str]] = ...,
        group_by: Optional[list[str]] = ...,
        having: Optional[list[str]] = ...,
        order_by: Optional[list[str]] = ...
) -> Iterator[tuple[_T, _T2, _T3]]: ...
@overload
def yield_from_easy_query(
        db: Union[Connection, Database],
        t1: Type[_T],
        t2: Type[_T2],
        t3: Type[_T3],
        t4: Type[_T4],
        /,
        blacklist: Iterable[str] = ...,
        accept_non_pk: bool = ...,
        distinct: bool = ...,
        where: Optional[list[str]] = ...,
        group_by: Optional[list[str]] = ...,
        having: Optional[list[str]] = ...,
        order_by: Optional[list[str]] = ...
) -> Iterator[tuple[_T, _T2, _T3, _T4]]: ...
@overload
def yield_from_easy_query(
        db: Union[Connection, Database],
        t1: Type[_T],
        t2: Type[_T2],
        t3: Type[_T3],
        t4: Type[_T4],
        t5: Type[_T5],
        /,
        blacklist: Iterable[str] = ...,
        accept_non_pk: bool = ...,
        distinct: bool = ...,
        where: Optional[list[str]] = ...,
        group_by: Optional[list[str]] = ...,
        having: Optional[list[str]] = ...,
        order_by: Optional[list[str]] = ...
) -> Iterator[tuple[_T, _T2, _T3, _T4, _T5]]: ...
@overload
def yield_from_easy_query(
        db: Union[Connection, Database],
        t1: Type[_T],
        t2: Type[_T2],
        t3: Type[_T3],
        t4: Type[_T4],
        t5: Type[_T5],
        t6: Type[_T6],
        /,
        blacklist: Iterable[str] = ...,
        accept_non_pk: bool = ...,
        distinct: bool = ...,
        where: Optional[list[str]] = ...,
        group_by: Optional[list[str]] = ...,
        having: Optional[list[str]] = ...,
        order_by: Optional[list[str]] = ...
) -> Iterator[tuple[_T, _T2, _T3, _T4, _T5, _T6]]: ...
@overload
def yield_from_easy_query(
        db: Union[Connection, Database],
        t1: Type[_T],
        t2: Type[_T2],
        t3: Type[_T3],
        t4: Type[_T4],
        t5: Type[_T5],
        t6: Type[_T6],
        t7: Type[_T7],
        /,
        blacklist: Iterable[str] = ...,
        accept_non_pk: bool = ...,
        distinct: bool = ...,
        where: Optional[list[str]] = ...,
        group_by: Optional[list[str]] = ...,
        having: Optional[list[str]] = ...,
        order_by: Optional[list[str]] = ...
) -> Iterator[tuple[_T, _T2, _T3, _T4, _T5, _T6, _T7]]: ...
@overload
def yield_from_easy_query(
        db: Union[Connection, Database],
        t1: Type[_T],
        t2: Type[_T2],
        t3: Type[_T3],
        t4: Type[_T4],
        t5: Type[_T5],
        t6: Type[_T6],
        t7: Type[_T7],
        t8: Type[_T8],
        /,
        blacklist: Iterable[str] = ...,
        accept_non_pk: bool = ...,
        distinct: bool = ...,
        where: Optional[list[str]] = ...,
        group_by: Optional[list[str]] = ...,
        having: Optional[list[str]] = ...,
        order_by: Optional[list[str]] = ...
) -> Iterator[tuple[_T, _T2, _T3, _T4, _T5, _T6, _T7, _T8]]: ...
@overload
def yield_from_easy_query(
        db: Union[Connection, Database],
        t1: Type[_T],
        t2: Type[_T2],
        t3: Type[_T3],
        t4: Type[_T4],
        t5: Type[_T5],
        t6: Type[_T6],
        t7: Type[_T7],
        t8: Type[_T8],
        t9: Type[_T9],
        /,
        blacklist: Iterable[str] = ...,
        accept_non_pk: bool = ...,
        distinct: bool = ...,
        where: Optional[list[str]] = ...,
        group_by: Optional[list[str]] = ...,
        having: Optional[list[str]] = ...,
        order_by: Optional[list[str]] = ...
) -> Iterator[tuple[_T, _T2, _T3, _T4, _T5, _T6, _T7, _T8, _T9]]: ...

def yield_from_easy_query(
        db: Union[Connection, Database],
        root: Any,
        *tables: Any,
        blacklist: Iterable[str] = ("BehaviorPack", "ResourcePack"),
        accept_non_pk: bool = True,
        distinct: bool = True,
        where: Optional[list[str]] = None,
        group_by: Optional[list[str]] = None,
        having: Optional[list[str]] = None,
        order_by: Optional[list[str]] = None) -> Iterator[tuple]:
    '''
    Returns an iterator that yields the wrapper classes from the query
    results. The arguments are the same as for :func:`build_easy_query`.
    The first argument is the Database object or the sqlite3.Connection.
    '''

    # Copy the arguments and pass them to build_easy_query
    sql_query = build_easy_query(
        root, *tables,
        blacklist=blacklist,
        accept_non_pk=accept_non_pk,
        distinct=distinct,
        where=where,
        group_by=group_by,
        having=having,
        order_by=order_by
    )
    yield from yield_from_any_query(db, sql_query)

def yield_from_any_query(db: Union[Connection, Database], sql_query: str) -> Iterator[tuple]:
    '''
    Yields the results from any query generated by :func:`build_easy_query`
    function and wraps them in the wrapper classes. This function in
    combination with :func:`build_easy_query` is equivalent to the
    :func:`yield_from_easy_query` function.
    '''
    if isinstance(db, Database):
        db = db.connection
    cursor = db.execute(sql_query)
    wrappers = [
        WRAPPER_CLASSES[d[0]] for d in cursor.description
    ]
    for row in cursor:
        yield tuple(
            None if value is None else wrapper(db, value)
            for value, wrapper in zip(row, wrappers)
        )

# Private functions
@dataclass
class _EasyQueryConnection:
    '''
    Represents a connection between two tables internally when searching for
    indirect connections in the database with :func:`_find_connection`.
    '''
    left: str
    left_column: str
    right: str
    right_column: str
    left_join: bool = False

def _find_connection(
        start: str, end: str, accept_non_pk: bool, visited: set[str]):
    '''
    Finds a connnection between two tables in the database.

    :param db: The database connection.
    :param start: The starting table.
    :param end: The ending table.
    :param accept_non_pk: Whether to accept non-primary key relations.
    :param visited: A list of tables to ignore while searching.
    '''
    queue: deque[tuple[str, list[_EasyQueryConnection]]] = deque([(start, [])])
    while len(queue) > 0:
        # Pop the first element (shortest path visited)
        node, path = queue.popleft()
        if node in visited:
            continue
        visited.add(node)

        # If node is the end, return the path
        if node == end:
            return path

        # Add all connections to the end of the queue
        for child, relation in RELATION_MAP.get(node, {}).items():
            if not accept_non_pk and not relation.is_pk:
                continue
            queue.append((child, path + [
                _EasyQueryConnection(
                    left=node,
                    left_column=relation.columns[0],
                    right=child,
                    right_column=relation.columns[1]
                )
            ]))
    return None

# Finalize building dynamic classes
assert validate_weak_connections()
add_reverse_connections()

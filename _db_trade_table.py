# pylint: disable=no-member, multiple-statements, missing-module-docstring, missing-class-docstring
from sqlite3 import Connection
from pathlib import Path
import json
from .better_json_tools import load_jsonc
from .utils import split_item_name
from ._views import dbtableview, WeakTableConnection

@dbtableview(
    properties={
        "path": (Path, "NOT NULL")
    },
    connects_to=["BehaviorPack"]
)
class TradeTableFile: ...

@dbtableview(
    properties={
        # Identifier of the trade table(path to the file relative to the behavior
        # pack root). Unike some other path based identifiers, this one includes
        # the file extension.
        "identifier": (str, "NOT NULL"),
    },
    connects_to=["TradeTableFile"]
)
class TradeTable: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "dataValue": (int, ""),
        "jsonPath": (str, "NOT NULL")
    },
    connects_to=["TradeTable"],
    weak_connects_to=[
        WeakTableConnection("identifier", "RpItem", "identifier"),
        WeakTableConnection("identifier", "BpItem", "identifier")
    ]
)
class TradeTableItemField: ...

@dbtableview(
    properties={
        "entityIdentifier": (str, "NOT NULL"),
        "spawnEggIdentifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL")
    },
    connects_to=["TradeTableItemField"],
    weak_connects_to=[
        WeakTableConnection("spawnEggIdentifier", "EntitySpawnEggField", "identifier")
    ]
)
class TradeTableItemSpawnEggReferenceField: ...

TRADE_TABLE_BUILD_SCRIPT: str = (
    TradeTableFile.build_script +
    TradeTable.build_script +
    TradeTableItemField.build_script +
    TradeTableItemSpawnEggReferenceField.build_script
)

def load_trade_tables(db: Connection, rp_id: int):
    '''
    Loads all trade tables from the behavior pack.
    '''
    rp_path: Path = db.execute(
        "SELECT path FROM BehaviorPack WHERE BehaviorPack_pk = ?",
        (rp_id,)
    ).fetchone()[0]

    for trade_table_path in (rp_path / "trading").rglob("*.json"):
        load_trade_table(db, trade_table_path, rp_path, rp_id)

def load_trade_table(
        db: Connection, trade_table_path: Path, rp_path: Path, rp_id: int):
    '''
    Loads a trade table from the behavior pack.
    '''
    cursor = db.cursor()
    # LOOT TABLE FILE
    cursor.execute(
        "INSERT INTO TradeTableFile (path, BehaviorPack_fk) VALUES (?, ?)",
        (trade_table_path.as_posix(), rp_id)
    )
    file_pk = cursor.lastrowid
    try:
        trade_table_jsonc = load_jsonc(trade_table_path)
    except json.JSONDecodeError:
        # sinlently skip invalid files. The file is in db but has no data
        return

    # LOOT TABLE
    identifier = trade_table_path.relative_to(rp_path).as_posix()
    cursor.execute(
        '''
        INSERT INTO TradeTable (
            identifier, TradeTableFile_fk
        ) VALUES (?, ?)
        ''',
        (identifier, file_pk)
    )
    trade_table_pk = cursor.lastrowid

    # LOOT TABLE ITEM FIELDS
    tier_walker = trade_table_jsonc / "tiers" // int
    trade_walker = (
        tier_walker / "groups" // int / "trades" // int +
        tier_walker / "trades" // int
    )
    gives_wants_walker = (
        trade_walker / "gives" // int +
        trade_walker / "wants" // int
    )
    item_walker = (
        gives_wants_walker +
        gives_wants_walker / "choice" // int
    )
    for iw in item_walker:
        # THE ITEM
        iw_item_walker = iw / "item"
        if not isinstance(iw_item_walker.data, str):
            continue
        namespace, name, data_value = split_item_name(iw_item_walker.data)
        cursor.execute(
            '''
            INSERT INTO TradeTableItemField (
                identifier, dataValue, jsonPath, TradeTable_fk
            ) VALUES (?, ?, ?, ?)
            ''',
            (f'{namespace}:{name}', data_value, iw.path_str, trade_table_pk)
        )
        item_pk = cursor.lastrowid
        # THE SPAWN EGG
        if name == 'spawn_egg':
            functions_walker = iw / "functions" // int
            for fw in functions_walker:
                function_name = fw / "function"
                if not (
                        isinstance(function_name.data, str) and
                        function_name.data == "set_actor_id"):
                    continue
                entity_identifier = fw / "id"
                if not isinstance(entity_identifier.data, str):
                    continue
                cursor.execute(
                    '''
                    INSERT INTO TradeTableItemSpawnEggReferenceField (
                        entityIdentifier, spawnEggIdentifier, jsonPath,
                        TradeTableItemField_fk
                    ) VALUES (?, ?, ?, ?)
                    ''',
                    (
                        entity_identifier.data,
                        entity_identifier.data + "_spawn_egg",
                        fw.path_str,
                        item_pk,
                    )
                )

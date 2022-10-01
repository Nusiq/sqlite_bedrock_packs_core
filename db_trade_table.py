from typing import cast, Optional
from sqlite3 import Connection
from pathlib import Path
from .better_json_tools import load_jsonc
from .utils import split_item_name
import json

TRADE_TABLE_BUILD_SCRIPT = '''
-- Trade Table
CREATE TABLE TradeTableFile (
    TradeTableFile_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    BehaviorPack_fk INTEGER,

    path Path NOT NULL,
    FOREIGN KEY (BehaviorPack_fk) REFERENCES BehaviorPack (BehaviorPack_pk)
        ON DELETE CASCADE
);
CREATE INDEX TradeTableFile_BehaviorPack_fk
ON TradeTableFile (BehaviorPack_fk);

CREATE TABLE TradeTable (
    TradeTable_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    TradeTableFile_fk INTEGER NOT NULL,

    -- Identifier of the trade table (path to the file relative to the behavior
    -- pack root). Unike some other path based identifiers, this one includes
    -- the file extension.
    identifier TEXT NOT NULL,

    FOREIGN KEY (TradeTableFile_fk) REFERENCES TradeTableFile (TradeTableFile_pk)
        ON DELETE CASCADE
);
CREATE INDEX TradeTable_TradeTableFile_fk
ON TradeTable (TradeTableFile_fk);

CREATE TABLE TradeTableItemField (
    -- A reference to an item inside a trade table.

    TradeTableItemField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    TradeTable_fk INTEGER NOT NULL,

    identifier TEXT NOT NULL,
    dataValue INTEGER,
    jsonPath TEXT NOT NULL,

    FOREIGN KEY (TradeTable_fk) REFERENCES TradeTable (TradeTable_pk)
        ON DELETE CASCADE
);
CREATE INDEX TradeTableItemField_TradeTable_fk
ON TradeTableItemField (TradeTable_fk);
'''

def load_trade_tables(db: Connection, rp_id: int):
    rp_path: Path = db.execute(
        "SELECT path FROM BehaviorPack WHERE BehaviorPack_pk = ?",
        (rp_id,)
    ).fetchone()[0]

    for trade_table_path in (rp_path / "trading").rglob("*.json"):
        load_trade_table(db, trade_table_path, rp_path, rp_id)

def load_trade_table(
        db: Connection, trade_table_path: Path, rp_path: Path, rp_id: int):
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
        gives_wants_walker / "item" +
        gives_wants_walker / "choice" // int / "item"
    )
    for iw in item_walker:
        if not isinstance(iw.data, str):
            continue
        namespace, name, data_value = split_item_name(iw.data)
        cursor.execute(
            '''
            INSERT INTO TradeTableItemField (
                identifier, dataValue, jsonPath, TradeTable_fk
            ) VALUES (?, ?, ?, ?)
            ''',
            (f'{namespace}:{name}', data_value, iw.path_str, trade_table_pk)
        )

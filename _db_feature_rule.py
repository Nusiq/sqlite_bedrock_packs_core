# pylint: disable=no-member, multiple-statements, missing-module-docstring, missing-class-docstring
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
class FeatureRuleFile: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "placesFeature": (str, "")
    },
    connects_to=["FeatureRuleFile"],
    weak_connects_to=[
        WeakTableConnection("placesFeature", "Feature", "identifier")
    ]
)
class FeatureRule: ...


FEATURE_RULE_BUILD_SCRIPT: str = (
    FeatureRuleFile.build_script +
    FeatureRule.build_script
)

def load_feature_rules(db: Connection, bp_id: int):
    '''
    Loads all feature rules from the behavior pack.
    '''
    bp_path: Path = db.execute(
        "SELECT path FROM BehaviorPack WHERE BehaviorPack_pk = ?",
        (bp_id,)
    ).fetchone()[0]

    for feature_rule_path in (bp_path / "feature_rules").glob("*.json"):
        load_feature_rule(db, feature_rule_path, bp_id)

def load_feature_rule(db: Connection, feature_rule_path: Path, bp_id: int):
    '''
    Loads a feature rule from the behavior pack.
    '''
    cursor = db.cursor()
    # FEATURE RULE FILE
    cursor.execute(
        "INSERT INTO FeatureRuleFile (path, BehaviorPack_fk) VALUES (?, ?)",
        (feature_rule_path.as_posix(), bp_id))

    file_pk = cursor.lastrowid
    try:
        feature_rule_jsonc = load_jsonc(feature_rule_path)
    except json.JSONDecodeError:
        # sinlently skip invalid files. The file is in db but has no data
        return

    feature_rule_walker = feature_rule_jsonc / "minecraft:feature_rules"
    description = feature_rule_walker / "description"

    # FEATURE RULE
    feature_rule_identifier = (description / "identifier").data
    if not isinstance(feature_rule_identifier, str):
        # Skip feature rules without identifiers
        return
    places_feature = (description / "places_feature").data
    if not isinstance(places_feature, str):
        places_feature = None
    cursor.execute(
        '''
        INSERT INTO FeatureRule (
        identifier, placesFeature, FeatureRuleFile_fk
        ) VALUES (?, ?, ?)
        ''',
        (feature_rule_identifier, places_feature, file_pk))

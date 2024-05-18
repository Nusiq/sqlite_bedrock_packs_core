# pylint: disable=no-member, multiple-statements, missing-module-docstring, missing-class-docstring
from sqlite3 import Connection
from pathlib import Path
import json
from .better_json_tools import load_jsonc, JSONSplitWalker
from ._views import dbtableview, WeakTableConnection

FEATURE_TYPES = [
    "minecraft:aggregate_feature",
    "minecraft:cave_carver_feature",
    "minecraft:fossil_feature",
    "minecraft:geode_feature",
    "minecraft:growing_plant_feature",
    "minecraft:nether_cave_carver_feature",
    "minecraft:multiface_feature",
    "minecraft:ore_feature",
    "minecraft:scatter_feature",
    "minecraft:search_feature",
    "minecraft:sequence_feature",
    "minecraft:single_block_feature",
    "minecraft:snap_to_surface_feature",
    "minecraft:surface_relative_threshold_feature",
    "minecraft:structure_template_feature",
    "minecraft:tree_feature",
    "minecraft:underwater_cave_carver_feature",
    "minecraft:vegetation_patch_feature",
    "minecraft:weighted_random_feature",
]

@dbtableview(
    properties={
        "path": (Path, "NOT NULL")
    },
    connects_to=["BehaviorPack"]
)
class FeatureFile: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL"),
    },
    enum_properties={
        # Could be deduced from the jsonPath, but this is more explicit
        "featureType": FEATURE_TYPES
    },
    connects_to=["FeatureFile"]
)
class Feature: ...


@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL")
    },

    # The PK/FK connection to Feature is handled externally (in table below)
    # because of the limitations of the @dbtableview decorator regarding
    # multiple connections to the same table.
    # connects_to=["Feature"] # DON'T DO THIS
    weak_connects_to=[
        WeakTableConnection("identifier", "Feature", "identifier")
    ]
)
class FeaturePlacesFeatureFieldValue: ...



# TODO - The problem is that you can't have multiple connections to the same
# table. The easy_query function wouldn't know which connection to use. In this
# particular case, both connections have a different meaning. The PK/FK
# connection means that the FeaturePlacesFeatureField is a field of its parent
# Feature. The weak identifier-based connection means that the Feature that
# owns the FeaturePlacesFeatureField places another Feature, referenced by its
# identifier.
@dbtableview(
    connects_to=["Feature", "FeaturePlacesFeatureFieldValue"],
)
class FeaturePlacesFeatureField:
    '''
    This class is used as a workaround for easy queries that enables to find
    out the Feature, that places a FeaturePlacesFeatureField. By default, when
    you make an easy query, that seeks the connections between Features and
    FeaturePlacesFeatureFields, you will get the Features that are placed by
    FeaturePlacesFeatureFields. 
    '''



FEATURE_BUILD_SCRIPT: str = (
    FeatureFile.build_script +
    Feature.build_script +
    FeaturePlacesFeatureFieldValue.build_script +
    FeaturePlacesFeatureField.build_script
)

def load_features(db: Connection, bp_id: int):
    '''
    Loads all features from the behavior pack.
    '''
    bp_path: Path = db.execute(
        "SELECT path FROM BehaviorPack WHERE BehaviorPack_pk = ?",
        (bp_id,)
    ).fetchone()[0]

    for feature_rule_path in (bp_path / "features").glob("*.json"):
        load_feature(db, feature_rule_path, bp_id)

def load_feature(db: Connection, feature_path: Path, bp_id: int):
    '''
    Loads a feature from the behavior pack.
    '''
    cursor = db.cursor()
    # FEATURE RULE FILE
    cursor.execute(
        "INSERT INTO FeatureFile (path, BehaviorPack_fk) VALUES (?, ?)",
        (feature_path.as_posix(), bp_id))

    file_pk = cursor.lastrowid
    try:
        feature_jsonc = load_jsonc(feature_path)
    except json.JSONDecodeError:
        # sinlently skip invalid files. The file is in db but has no data
        return

    for feature_walker in feature_jsonc // str:
        feature_type = feature_walker.parent_key
        if feature_type not in FEATURE_TYPES:
            # Skip feature rules with invalid types
            continue


        # FEATURE
        description = feature_walker / "description"
        feature_rule_identifier = (description / "identifier").data
        if not isinstance(feature_rule_identifier, str):
            # Skip feature rules without identifiers
            return
        cursor.execute(
            '''
            INSERT INTO Feature (
            identifier, jsonPath, featureType, FeatureFile_fk
            ) VALUES (?, ?, ?, ?)
            ''',
            (
                feature_rule_identifier, feature_walker.path_str,
                feature_type, file_pk
            )
        )
        feature_pk = cursor.lastrowid
        
        # FEATURE PLACES FEATURE FIELD
        # Depends on the feature type. Gets a JSONSplitWalker that lists all
        # of placed features.
        places_features_walker: JSONSplitWalker
        if feature_type in [
                "minecraft:aggregate_feature", "minecraft:sequence_feature"]:
            places_features_walker = feature_walker / "features" // int
        elif feature_type in [
                "minecraft:scatter_feature", "minecraft:search_feature"]:
            pfw = feature_walker / "places_feature"
            if not pfw.exists:
                continue
            places_features_walker =  JSONSplitWalker([pfw])
        elif feature_type == "minecraft:snap_to_surface_feature":
            pfw = feature_walker / "feature_to_snap"
            if not pfw.exists:
                continue
            places_features_walker =  JSONSplitWalker([pfw])
        elif feature_type == "minecraft:surface_relative_threshold_feature":
            pfw = feature_walker / "feature_to_place"
            if not pfw.exists:
                continue
            places_features_walker =  JSONSplitWalker([pfw])
        elif feature_type == "minecraft:vegetation_patch_feature":
            pfw = feature_walker / "vegetation_feature"
            if not pfw.exists:
                continue
            places_features_walker =  JSONSplitWalker([pfw])
        elif feature_type == "minecraft:weighted_random_feature":
            places_features_walker = feature_walker / "features" // int / 0
        else:
            continue
        
        for places_feature_walker in places_features_walker:
            if not isinstance(places_feature_walker.data, str):
                continue
            cursor.execute(
                '''
                INSERT INTO FeaturePlacesFeatureFieldValue (
                identifier, jsonPath
                ) VALUES (?, ?)
                ''',
                (
                    places_feature_walker.data, places_feature_walker.path_str,
                )
            )
            feature_places_feature_field_value_pk = cursor.lastrowid
            cursor.execute(
                '''
                INSERT INTO FeaturePlacesFeatureField (
                Feature_fk, FeaturePlacesFeatureFieldValue_fk
                ) VALUES (?, ?)
                ''',
                (feature_pk, feature_places_feature_field_value_pk)
            )
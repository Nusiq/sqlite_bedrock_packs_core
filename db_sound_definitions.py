from typing import cast, Optional
from sqlite3 import Connection
from pathlib import Path
from .better_json_tools import load_jsonc
from .decorators import dbtableview
import json

@dbtableview(
    properties={
        "path": (Path, "NOT NULL")
    },
    connects_to=["ResourcePack"]
)
class SoundDefinitionsFile: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL")
    },
    connects_to=["SoundDefinitionsFile"]
)
class SoundDefinition: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL")
    },
    connects_to=["SoundDefinition"],
    weak_connects_to=[
        ("identifier", "SoundFile", "identifier")
    ]
)
class SoundDefinitionSoundField: ...

SOUND_DEFINITIONS_BUILD_SCRIPT = (
    SoundDefinitionsFile.build_script +
    SoundDefinition.build_script +
    SoundDefinitionSoundField.build_script
)

def load_sound_definitions(db: Connection, rp_id: int):
    rp_path: Path = db.execute(
        "SELECT path FROM ResourcePack WHERE ResourcePack_pk = ?",
        (rp_id,)
    ).fetchone()[0]

    sound_definitions_path = rp_path / "sounds" / "sound_definitions.json"
    if sound_definitions_path.exists():
        load_sound_definition(db, sound_definitions_path, rp_id)


def load_sound_definition(db: Connection, sound_definition_path: Path, rp_id: int):
    cursor = db.cursor()
    # GEOMETRY FILE
    cursor.execute(
        "INSERT INTO SoundDefinitionsFile (path, ResourcePack_fk) VALUES (?, ?)",
        (sound_definition_path.as_posix(), rp_id)
    )
    file_pk = cursor.lastrowid
    try:
        sound_definitions_jsonc = load_jsonc(sound_definition_path)
    except json.JSONDecodeError:
        # sinlently skip invalid files. The file is in db but has no data
        return
    # Try to load from the "sound_definitions" key, if it doesn't exist,
    # load from the root of the file (old format)
    if (sound_definitions_jsonc / "sound_definitions").exists:
        sound_list = sound_definitions_jsonc / "sound_definitions" // str
        old_format = False
    else:
        sound_list = sound_definitions_jsonc // str
        old_format = True
    for sound_definition in sound_list:
        if old_format and sound_definition.parent_key == "format_version":
            continue
        sound_definition_identifier = cast(str, sound_definition.parent_key)
        cursor.execute(
            '''
            INSERT INTO SoundDefinition (
                identifier, jsonPath, SoundDefinitionsFile_fk
            ) VALUES (?, ?, ?)
            ''',
            (sound_definition_identifier, sound_definition.path_str, file_pk)
        )
        sound_definition_pk = cursor.lastrowid
        for sound in sound_definition / "sounds" // int:
            if isinstance(sound.data, str):
                sound_identifier = sound.data
            elif isinstance(sound.data, dict):
                sound_name_walker = sound / "name"
                if (
                        not sound_name_walker.exists or
                        not isinstance(sound_name_walker.data, str)):
                    continue
                sound_identifier = sound_name_walker.data
            else:
                continue
            cursor.execute(
                '''
                INSERT INTO SoundDefinitionSoundField (
                    identifier, jsonPath, SoundDefinition_fk
                ) VALUES (?, ?, ?)
                ''',
                (sound_identifier, sound.path_str, sound_definition_pk)
            )

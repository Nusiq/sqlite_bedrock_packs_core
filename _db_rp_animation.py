from typing import cast
from sqlite3 import Connection
from pathlib import Path
from .better_json_tools import load_jsonc
from ._views import dbtableview
import json

@dbtableview(
    properties={
        "path": (Path, "NOT NULL")
    },
    connects_to=["ResourcePack"]
)
class RpAnimationFile: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL")
    },
    connects_to=["RpAnimationFile"]
)
class RpAnimation: ...

@dbtableview(
    properties={
        "shortName": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL")
    },
    connects_to=["RpAnimation"]
)
class RpAnimationParticleEffect: ...

@dbtableview(
    properties={
        "shortName": (str, "NOT NULL"),
        "jsonPath": (str, "NOT NULL")
    },
    connects_to=["RpAnimation"]
)
class RpAnimationSoundEffect: ...

RP_ANIMATION_BUILD_SCRIPT: str = (
    RpAnimationFile.build_script +
    RpAnimation.build_script +
    RpAnimationParticleEffect.build_script +
    RpAnimationSoundEffect.build_script
)

def load_rp_animations(db: Connection, rp_id: int):
    rp_path: Path = db.execute(
        "SELECT path FROM ResourcePack WHERE ResourcePack_pk = ?",
        (rp_id,)
    ).fetchone()[0]

    for animation_path in (rp_path / "animations").rglob("*.json"):
        load_rp_animation(db, animation_path, rp_id)

def load_rp_animation(db: Connection, animation_path: Path, rp_id: int):
    cursor = db.cursor()
    # RP ANIMATION FILE
    cursor.execute(
        "INSERT INTO RpAnimationFile (path, ResourcePack_fk) VALUES (?, ?)",
        (animation_path.as_posix(), rp_id)
    )
    file_pk = cursor.lastrowid
    try:
        animations_walker = load_jsonc(animation_path)
    except json.JSONDecodeError:
        # sinlently skip invalid files. The file is in db but has no data
        return

    for animation_walker in animations_walker / "animations" // str:
        identifier_data: str = cast(str, animation_walker.parent_key)
        if not identifier_data.startswith("animation."):
            continue
        cursor.execute(
            '''
            INSERT INTO RpAnimation (
                RpAnimationFile_fk, identifier, jsonPath
            ) VALUES (?, ?, ?)
            ''',
            (file_pk, identifier_data, animation_walker.path_str)
        )
        rpanim_pk = cursor.lastrowid
        # LOAD PARTICLE EFFECTS
        for particle_effect_walker in (
                animation_walker / "particle_effects" // str):
            short_name = particle_effect_walker / "effect"
            if not isinstance(short_name.data, str):
                continue
            cursor.execute(
                '''
                INSERT INTO RpAnimationParticleEffect (
                    RpAnimation_fk, shortName, jsonPath
                ) VALUES (?, ?, ?)
                ''',
                (rpanim_pk, short_name.data, particle_effect_walker.path_str)
            )
        # LOAD SOUND EFFECTS
        for sound_effect_walker in animation_walker / "sound_effects" // str:
            short_name = sound_effect_walker / "effect"
            if not isinstance(short_name.data, str):
                continue
            cursor.execute(
                '''
                INSERT INTO RpAnimationSoundEffect (
                    RpAnimation_fk, shortName, jsonPath
                ) VALUES (?, ?, ?)
                ''',
                (rpanim_pk, short_name.data, sound_effect_walker.path_str)
            )


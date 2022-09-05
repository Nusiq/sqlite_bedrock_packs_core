from sqlite3 import Connection
from pathlib import Path
from .better_json import load_jsonc
import json

RP_ANIMATION_CONTROLLER_BUILD_SCRIPT = '''
-- RpAnimationController
CREATE TABLE RpAnimationControllerFile (
    RpAnimationControllerFile_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    ResourcePack_fk INTEGER,

    path Path NOT NULL,
    FOREIGN KEY (ResourcePack_fk) REFERENCES ResourcePack (ResourcePack_pk)
        ON DELETE CASCADE
);
CREATE INDEX RpAnimationControllerFile_ResourcePack_fk
ON RpAnimationControllerFile (ResourcePack_fk);

CREATE TABLE RpAnimationController (
    RpAnimationController_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    RpAnimationControllerFile_fk INTEGER NOT NULL,

    identifier TEXT NOT NULL,
    jsonPath TEXT NOT NULL,
    
    FOREIGN KEY (RpAnimationControllerFile_fk) REFERENCES RpAnimationControllerFile (RpAnimationControllerFile_pk)
        ON DELETE CASCADE
);
CREATE INDEX RpAnimationController_RpAnimationControllerFile_fk
ON RpAnimationController (RpAnimationControllerFile_fk);

CREATE TABLE RpAnimationControllerParticleEffect (
    RpAnimationControllerParticleEffect_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    RpAnimationController_fk INTEGER NOT NULL,

    shortName TEXT NOT NULL,
    jsonPath TEXT NOT NULL,

    FOREIGN KEY (RpAnimationController_fk) REFERENCES RpAnimationController (RpAnimationController_pk)
        ON DELETE CASCADE
);
CREATE INDEX RpAnimationControllerParticleEffect_RpAnimationController_fk
ON RpAnimationControllerParticleEffect (RpAnimationController_fk);

CREATE TABLE RpAnimationControllerSoundEffect (
    RpAnimationControllerSoundEffect_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    RpAnimationController_fk INTEGER NOT NULL,

    shortName TEXT NOT NULL,
    jsonPath TEXT NOT NULL,

    FOREIGN KEY (RpAnimationController_fk) REFERENCES RpAnimationController (RpAnimationController_pk)
        ON DELETE CASCADE
);
CREATE INDEX RpAnimationControllerSoundEffect_RpAnimationController_fk
ON RpAnimationControllerSoundEffect (RpAnimationController_fk);
'''

def load_rp_animation_controllers(db: Connection, rp_id: int):
    rp_path: Path = db.execute(
        "SELECT path FROM ResourcePack WHERE ResourcePack_pk = ?",
        (rp_id,)
    ).fetchone()[0]

    for ac_path in (rp_path / "animation_controllers").rglob("*.json"):
        load_rp_animation_controller(db, ac_path, rp_id)


def load_rp_animation_controller(db: Connection, animation_controller_path: Path, rp_id: int):
    cursor = db.cursor()
    # RP ANIMATION FILE
    cursor.execute(
        "INSERT INTO RpAnimationControllerFile (path, ResourcePack_fk) VALUES (?, ?)",
        (animation_controller_path.as_posix(), rp_id)
    )
    file_pk = cursor.lastrowid
    try:
        acs_walker = load_jsonc(animation_controller_path)
    except json.JSONDecodeError:
        # sinlently skip invalid files. The file is in db but has no data
        return

    for ac_walker in acs_walker / "animation_controllers" // str:
        if not ac_walker.parent_key.startswith("controller.animation."):
            continue
        identifier_data = ac_walker.parent_key
        cursor.execute(
            '''
            INSERT INTO RpAnimationController (
                RpAnimationControllerFile_fk, identifier, jsonPath
            ) VALUES (?, ?, ?)
            ''',
            (file_pk, identifier_data, ac_walker.path_str)
        )
        rpanim_pk = cursor.lastrowid

        states = ac_walker / "states" // str
        # LOAD PARTICLE EFFECTS
        for particle_effect_walker in states / "particle_effects" // int:
            short_name = particle_effect_walker / "effect"
            if not isinstance(short_name.data, str):
                continue
            cursor.execute(
                '''
                INSERT INTO RpAnimationControllerParticleEffect (
                    RpAnimationController_fk, shortName, jsonPath
                ) VALUES (?, ?, ?)
                ''',
                (rpanim_pk, short_name.data, particle_effect_walker.path_str)
            )
        # LOAD SOUND EFFECTS
        for sound_effect_walker in states / "sound_effects" // int:
            short_name = sound_effect_walker / "effect"
            if not isinstance(short_name.data, str):
                continue
            cursor.execute(
                '''
                INSERT INTO RpAnimationControllerSoundEffect (
                    RpAnimationController_fk, shortName, jsonPath
                ) VALUES (?, ?, ?)
                ''',
                (rpanim_pk, short_name.data, sound_effect_walker.path_str)
            )

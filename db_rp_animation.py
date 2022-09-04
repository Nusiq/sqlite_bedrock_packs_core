from sqlite3 import Connection
from pathlib import Path
from .better_json import load_jsonc
import json

RP_ANIMATION_BUILD_SCRIPT = '''
-- RpAnimation
CREATE TABLE RpAnimationFile (
    RpAnimationFile_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    ResourcePack_fk INTEGER,

    path Path,
    FOREIGN KEY (ResourcePack_fk) REFERENCES ResourcePack (ResourcePack_pk)
        ON DELETE CASCADE
);
CREATE INDEX RpAnimationFile_ResourcePack_fk
ON RpAnimationFile (ResourcePack_fk);

CREATE TABLE RpAnimation (
    RpAnimation_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    RpAnimationFile_fk INTEGER NOT NULL,

    identifier TEXT,
    jsonPath TEXT,
    
    FOREIGN KEY (RpAnimationFile_fk) REFERENCES RpAnimationFile (RpAnimationFile_pk)
        ON DELETE CASCADE
);
CREATE INDEX RpAnimation_RpAnimationFile_fk
ON RpAnimation (RpAnimationFile_fk);

CREATE TABLE RpAnimationParticleEffect (
    RpAnimationParticleEffect_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    RpAnimation_fk INTEGER NOT NULL,

    shortName TEXT,
    jsonPath TEXT,

    FOREIGN KEY (RpAnimation_fk) REFERENCES RpAnimation (RpAnimation_pk)
        ON DELETE CASCADE
);
CREATE INDEX RpAnimationParticleEffect_RpAnimation_fk
ON RpAnimationParticleEffect (RpAnimation_fk);

CREATE TABLE RpAnimationSoundEffect (
    RpAnimationSoundEffect_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    RpAnimation_fk INTEGER NOT NULL,

    shortName TEXT,
    jsonPath TEXT,

    FOREIGN KEY (RpAnimation_fk) REFERENCES RpAnimation (RpAnimation_pk)
        ON DELETE CASCADE
);
CREATE INDEX RpAnimationSoundEffect_RpAnimation_fk
ON RpAnimationSoundEffect (RpAnimation_fk);
'''

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
        if not animation_walker.parent_key.startswith("animation."):
            continue
        identifier_data = animation_walker.parent_key
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


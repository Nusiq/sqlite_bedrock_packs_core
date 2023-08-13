from sqlite3 import Connection
from pathlib import Path
from .decorators import dbtableview

@dbtableview(
    properties={
        "path": (Path, "NOT NULL"),

        # The identifier is the path without extension. This is added to the DB to
        # make searches easier.
        "identifier": (str, "NOT NULL")
    },
    connects_to=["ResourcePack"]
)
class SoundFile: ...

SOUND_BUILD_SCRIPT = SoundFile.build_script

def load_sounds(db: Connection, rp_id: int):
    rp_path: Path = db.execute(
        "SELECT path FROM ResourcePack WHERE ResourcePack_pk = ?",
        (rp_id,)
    ).fetchone()[0]

    for sound_path in (rp_path / "sounds").rglob("*.wav"):
        load_sound(db, sound_path, rp_path, rp_id)
    for sound_path in (rp_path / "sounds").rglob("*.ogg"):
        load_sound(db, sound_path, rp_path, rp_id)
    for sound_path in (rp_path / "sounds").rglob("*.fsb"):
        load_sound(db, sound_path, rp_path, rp_id)

def load_sound(db: Connection, sound_path: Path, rp_path: Path, rp_id: int):
    cursor = db.cursor()
    # SOUND FILE AND ITS IDENTIFIER
    cursor.execute(
        """
        INSERT INTO SoundFile (
            path, identifier, ResourcePack_fk
        ) VALUES (?, ?, ?)
        """,
        (
            sound_path.as_posix(),
            sound_path.relative_to(rp_path).with_suffix("").as_posix(),
            rp_id
        )
    )
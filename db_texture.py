from sqlite3 import Connection
from pathlib import Path


TEXTURE_BUILD_SCRIPT = '''
-- Texture
CREATE TABLE TextureFile (
    TextureFile_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    ResourcePack_fk INTEGER,

    path Path NOT NULL,
    -- The identifier is the path without extension. This is added to the DB to
    -- make searches easier.
    identifier TEXT NOT NULL,
    FOREIGN KEY (ResourcePack_fk) REFERENCES ResourcePack (ResourcePack_pk)
        ON DELETE CASCADE
);
CREATE INDEX TextureFile_ResourcePack_fk
ON TextureFile (ResourcePack_fk);
'''

def load_textures(db: Connection, rp_id: int):
    rp_path: Path = db.execute(
        "SELECT path FROM ResourcePack WHERE ResourcePack_pk = ?",
        (rp_id,)
    ).fetchone()[0]

    for texture_path in (rp_path / "textures").rglob("*.png"):
        load_texture(db, texture_path, rp_path, rp_id)
    for texture_path in (rp_path / "textures").rglob("*.tga"):
        load_texture(db, texture_path, rp_path, rp_id)
    for texture_path in (rp_path / "textures").rglob("*.jpg"):
        load_texture(db, texture_path, rp_path, rp_id)

def load_texture(db: Connection, texture_path: Path, rp_path: Path, rp_id: int):
    cursor = db.cursor()
    # TEXTURE FILE AND ITS IDENTIFIER
    cursor.execute(
        """
        INSERT INTO TextureFile (
            path, identifier, ResourcePack_fk
        ) VALUES (?, ?, ?)
        """,
        (
            texture_path.as_posix(),
            texture_path.relative_to(rp_path).with_suffix("").as_posix(),
            rp_id
        )
    )
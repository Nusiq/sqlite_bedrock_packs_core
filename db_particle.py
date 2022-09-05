from sqlite3 import Connection
from pathlib import Path
from .better_json import load_jsonc
import json

PARTICLE_BUILD_SCRIPT = '''
-- Particle
CREATE TABLE ParticleFile (
    ParticleFile_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    ResourcePack_fk INTEGER,

    path Path,
    FOREIGN KEY (ResourcePack_fk) REFERENCES ResourcePack (ResourcePack_pk)
        ON DELETE CASCADE
);
CREATE INDEX ParticleFile_ResourcePack_fk
ON ParticleFile (ResourcePack_fk);

CREATE TABLE Particle (
    Particle_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    ParticleFile_fk INTEGER NOT NULL,

    identifier TEXT,
    material TEXT,
    texture TEXT,
    FOREIGN KEY (ParticleFile_fk) REFERENCES ParticleFile (ParticleFile_pk)
        ON DELETE CASCADE
);
CREATE INDEX Particle_ParticleFile_fk
ON Particle (ParticleFile_fk);
'''

def load_particles(db: Connection, rp_id: int):
    rp_path: Path = db.execute(
        "SELECT path FROM ResourcePack WHERE ResourcePack_pk = ?",
        (rp_id,)
    ).fetchone()[0]

    for particle_path in (rp_path / "particles").rglob("*.json"):
        load_particle(db, particle_path, rp_id)

def load_particle(db: Connection, particle_path: Path, rp_id: int):
    cursor = db.cursor()
    # PARTICLE FILE
    cursor.execute(
        "INSERT INTO ParticleFile (path, ResourcePack_fk) VALUES (?, ?)",
        (particle_path.as_posix(), rp_id)
    )
    file_pk = cursor.lastrowid
    try:
        particle_walker = load_jsonc(particle_path)
    except json.JSONDecodeError:
        # sinlently skip invalid files. The file is in db but has no data
        return
    description = particle_walker / "particle_effect" / "description"
    basic_render_parameters = description / "basic_render_parameters"

    identifier = description / "identifier"
    material = basic_render_parameters / "material"
    texture = basic_render_parameters / "texture"

    identifier_data = identifier.data
    if not isinstance(identifier_data, str):
        identifier_data = None
    material_data = material.data
    if not isinstance(material.data, str):
        material_data = None
    texture_data = texture.data
    if not isinstance(texture.data, str):
        texture_data = None
    cursor.execute(
        '''
        INSERT INTO Particle (
            identifier, material, texture, ParticleFile_fk
        ) VALUES (?, ?, ?, ?)
        ''',
        (identifier_data, material_data, texture_data, file_pk)
    )

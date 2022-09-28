from sqlite3 import Connection
from pathlib import Path
from .better_json_tools import load_jsonc
import json

ATTACHABLE_BUILD_SCRIPT = '''
-- Attachable
CREATE TABLE AttachableFile (
    AttachableFile_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    ResourcePack_fk INTEGER,

    path Path NOT NULL,
    FOREIGN KEY (ResourcePack_fk) REFERENCES ResourcePack (ResourcePack_pk)
        ON DELETE CASCADE
);
CREATE INDEX AttachableFile_ResourcePack_fk
ON AttachableFile (ResourcePack_fk);

CREATE TABLE Attachable (
    Attachable_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    AttachableFile_fk INTEGER NOT NULL,

    identifier TEXT NOT NULL,

    FOREIGN KEY (AttachableFile_fk) REFERENCES AttachableFile (AttachableFile_pk)
        ON DELETE CASCADE
);
CREATE INDEX Attachable_AttachableFile_fk
ON Attachable (AttachableFile_fk);

-- This maps the item used by the attachable. Attachables have two types of
-- connecting items. They can either have an identifier matching to the item
-- or they can have the 'item' property which maps the item with a condition.
-- In both cases this field is added to the database.
CREATE TABLE AttachableItemField (
    AttachableItemField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    Attachable_fk INTEGER NOT NULL,

    identifier TEXT NOT NULL,
    condition TEXT,
    jsonPath TEXT NOT NULL,

    FOREIGN KEY (Attachable_fk) REFERENCES Attachable (Attachable_pk)
        ON DELETE CASCADE
);
CREATE INDEX AttachableItemField_Attachable_fk
ON AttachableItemField (Attachable_fk);

CREATE TABLE AttachableMaterialField (
    AttachableMaterialField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    Attachable_fk INTEGER NOT NULL,

    shortName TEXT NOT NULL,
    identifier TEXT NOT NULL,
    jsonPath TEXT NOT NULL,

    FOREIGN KEY (Attachable_fk) REFERENCES Attachable (Attachable_pk)
        ON DELETE CASCADE
);
CREATE INDEX AttachableMaterialField_Attachable_fk
ON AttachableMaterialField (Attachable_fk);

CREATE TABLE AttachableTextureField (
    AttachableTextureField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    Attachable_fk INTEGER NOT NULL,

    shortName TEXT NOT NULL,
    identifier TEXT NOT NULL,
    jsonPath TEXT NOT NULL,

    FOREIGN KEY (Attachable_fk) REFERENCES Attachable (Attachable_pk)
        ON DELETE CASCADE
);
CREATE INDEX AttachableTextureField_Attachable_fk
ON AttachableTextureField (Attachable_fk);

CREATE TABLE AttachableGeometryField (
    AttachableGeometryField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    Attachable_fk INTEGER NOT NULL,

    shortName TEXT NOT NULL,
    identifier TEXT NOT NULL,
    jsonPath TEXT NOT NULL,

    FOREIGN KEY (Attachable_fk) REFERENCES Attachable (Attachable_pk)
        ON DELETE CASCADE
);
CREATE INDEX AttachableGeometryField_Attachable_fk
ON AttachableGeometryField (Attachable_fk);


CREATE TABLE AttachableRenderControllerField (
    AttachableRenderControllerField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    Attachable_fk INTEGER NOT NULL,

    identifier TEXT NOT NULL,
    condition TEXT,
    jsonPath TEXT NOT NULL,

    FOREIGN KEY (Attachable_fk) REFERENCES Attachable (Attachable_pk)
        ON DELETE CASCADE
);
CREATE INDEX AttachableRenderControllerField_Attachable_fk
ON AttachableRenderControllerField (Attachable_fk);


CREATE TABLE AttachableAnimationField (
    AttachableAnimationField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    Attachable_fk INTEGER NOT NULL,

    shortName TEXT NOT NULL,
    identifier TEXT NOT NULL,
    jsonPath TEXT NOT NULL,

    FOREIGN KEY (Attachable_fk) REFERENCES Attachable (Attachable_pk)
        ON DELETE CASCADE
);
CREATE INDEX AttachableAnimationField_Attachable_fk
ON AttachableAnimationField (Attachable_fk);

CREATE TABLE AttachableAnimationControllerField (
    AttachableAnimationControllerField_pk INTEGER PRIMARY KEY AUTOINCREMENT,
    Attachable_fk INTEGER NOT NULL,

    shortName TEXT NOT NULL,
    identifier TEXT NOT NULL,
    jsonPath TEXT NOT NULL,

    FOREIGN KEY (Attachable_fk) REFERENCES Attachable (Attachable_pk)
        ON DELETE CASCADE
);
CREATE INDEX AttachableAnimationControllerField_Attachable_fk
ON AttachableAnimationControllerField (Attachable_fk);
'''

def load_attachables(db: Connection, rp_id: int):
    rp_path: Path = db.execute(
        "SELECT path FROM ResourcePack WHERE ResourcePack_pk = ?",
        (rp_id,)
    ).fetchone()[0]

    for attachable_path in (rp_path / "attachables").rglob("*.json"):
        load_attachable(db, attachable_path, rp_id)

def load_attachable(db: Connection, attachable_path: Path, rp_id: int):
    cursor = db.cursor()
    # ATTACHABLE FILE
    cursor.execute(
        "INSERT INTO AttachableFile (path, ResourcePack_fk) VALUES (?, ?)",
        (attachable_path.as_posix(), rp_id)
    )
    file_pk = cursor.lastrowid
    try:
        walker = load_jsonc(attachable_path)
    except json.JSONDecodeError:
        # sinlently skip invalid files. The file is in db but has no data
        return
    # ATTACHABLE
    description = walker / "minecraft:attachable" / "description"
    identifier_walker = description / 'identifier'
    identifier_data = identifier_walker.data
    if not isinstance(identifier_data, str):
        return
    cursor.execute(
        '''
        INSERT INTO Attachable (
            identifier, AttachableFile_fk
        ) VALUES (?, ?)
        ''',
        (identifier_data, file_pk)
    )
    attachable_pk = cursor.lastrowid

    # ATTACHABLE ITEM FIELD
    items = description / "item" // str
    if len(items) == 0:
        cursor.execute(
            '''
            INSERT INTO AttachableItemField (
                identifier, Attachable_fk, jsonPath
            ) VALUES (?, ?, ?)
            ''',
            (identifier_data, attachable_pk, identifier_walker.path_str)
        )
    else:
        for item in items:
            if not isinstance(item.data, str):
                continue
            cursor.execute(
                '''
                INSERT INTO AttachableItemField (
                    identifier, Attachable_fk, condition,
                    jsonPath
                ) VALUES (?, ?, ?, ?)
                ''',
                (
                    item.parent_key, attachable_pk,
                    item.data, item.path_str
                )
            )

    # ATTACHABLE MATERIAL FIELD
    materials = description / "materials" // str
    for material in materials:
        if not isinstance(material.data, str):
            continue
        cursor.execute(
            '''
            INSERT INTO AttachableMaterialField (
                shortName, identifier, Attachable_fk, jsonPath
            ) VALUES (?, ?, ?, ?)
            ''',
            (
                material.parent_key, material.data, attachable_pk,
                material.path_str
            )
        )

    # ATTACHABLE TEXTURE FIELD
    textures = description / "textures" // str
    for texture in textures:
        if not isinstance(texture.data, str):
            continue
        cursor.execute(
            '''
            INSERT INTO AttachableTextureField (
                shortName, identifier, Attachable_fk, jsonPath
            ) VALUES (?, ?, ?, ?)
            ''',
            (
                texture.parent_key, texture.data, attachable_pk,
                texture.path_str
            )
        )

    # ATTACHABLE GEOMETRY FIELD
    geometries = description / "geometry" // str
    for geometry in geometries:
        if not isinstance(geometry.data, str):
            continue
        cursor.execute(
            '''
            INSERT INTO AttachableGeometryField (
                shortName, identifier, Attachable_fk, jsonPath
            ) VALUES (?, ?, ?, ?)
            ''',
            (
                geometry.parent_key, geometry.data, attachable_pk,
                geometry.path_str
            )
        )

    # ATTACHABLE RENDER CONTROLLER FIELD
    render_controllers = description / "render_controllers" // int
    for render_controller in render_controllers:
        if isinstance(render_controller.data, str):
            cursor.execute(
                '''
                INSERT INTO AttachableRenderControllerField (
                    identifier, Attachable_fk, jsonPath
                ) VALUES (?, ?, ?)
                ''',
                (
                    render_controller.data, attachable_pk,
                    render_controller.path_str
                )
            )
        else:
            # Render contoroller can be an object with pair of values, name
            # and condition.
            for render_controller in render_controller // str:
                cursor.execute(
                    '''
                    INSERT INTO AttachableRenderControllerField (
                        identifier, condition, Attachable_fk, jsonPath
                    ) VALUES (?, ?, ?, ?)
                    ''',
                    (
                        render_controller.parent_key, render_controller.data,
                        attachable_pk, render_controller.path_str
                    )
                )
    # ANIMATIONS & ANIMATION CONTROLLERS
    for animation in description / "animations" // str:
        if isinstance(animation.data, str):
            identifier = animation.data
        else:
            continue
        if animation.data.startswith("controller.animation."):
            # Animations
            cursor.execute(
                '''
                INSERT INTO AttachableAnimationControllerField (
                    Attachable_fk, shortName, identifier, jsonPath
                ) VALUES (?, ?, ?, ?)
                ''',
                (attachable_pk, animation.parent_key, identifier, animation.path_str))
        elif animation.data.startswith("animation."):
            # Animation Controllers

            cursor.execute(
                '''
                INSERT INTO AttachableAnimationField (
                    Attachable_fk, shortName, identifier, jsonPath
                ) VALUES (?, ?, ?, ?)
                ''',
                (attachable_pk, animation.parent_key, identifier, animation.path_str))

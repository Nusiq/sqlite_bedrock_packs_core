"""
THIS FILE IS GENERATED. DO NOT EDIT IT MANUALLY.
"""
from __future__ import annotations
from functools import cache
from sqlite3 import Connection
from pathlib import Path

class ResourcePack:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.ResourcePack_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT path
            FROM ResourcePack
            WHERE ResourcePack_pk = ?
            ''',
            (self.ResourcePack_pk,)
        ).fetchone()

    @property
    def path(self) -> Path:
        return self.query_result()[0]


class ClientEntityFile:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.ClientEntityFile_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ResourcePack_fk, path
            FROM ClientEntityFile
            WHERE ClientEntityFile_pk = ?
            ''',
            (self.ClientEntityFile_pk,)
        ).fetchone()

    @property
    def ResourcePack_fk(self) -> int | None:
        return self.query_result()[0]

    @property
    def path(self) -> Path:
        return self.query_result()[1]


class ClientEntity:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.ClientEntity_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ClientEntityFile_fk, identifier
            FROM ClientEntity
            WHERE ClientEntity_pk = ?
            ''',
            (self.ClientEntity_pk,)
        ).fetchone()

    @property
    def ClientEntityFile_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]


class ClientEntityRenderControllerField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.ClientEntityRenderControllerField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ClientEntity_fk, condition, identifier, jsonPath
            FROM ClientEntityRenderControllerField
            WHERE ClientEntityRenderControllerField_pk = ?
            ''',
            (self.ClientEntityRenderControllerField_pk,)
        ).fetchone()

    @property
    def ClientEntity_fk(self) -> int:
        return self.query_result()[0]

    @property
    def condition(self) -> str | None:
        return self.query_result()[1]

    @property
    def identifier(self) -> str:
        return self.query_result()[2]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[3]


class ClientEntityGeometryField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.ClientEntityGeometryField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ClientEntity_fk, identifier, jsonPath, shortName
            FROM ClientEntityGeometryField
            WHERE ClientEntityGeometryField_pk = ?
            ''',
            (self.ClientEntityGeometryField_pk,)
        ).fetchone()

    @property
    def ClientEntity_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]

    @property
    def shortName(self) -> str:
        return self.query_result()[3]


class ClientEntityTextureField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.ClientEntityTextureField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ClientEntity_fk, identifier, jsonPath, shortName
            FROM ClientEntityTextureField
            WHERE ClientEntityTextureField_pk = ?
            ''',
            (self.ClientEntityTextureField_pk,)
        ).fetchone()

    @property
    def ClientEntity_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]

    @property
    def shortName(self) -> str:
        return self.query_result()[3]


class ClientEntityMaterialField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.ClientEntityMaterialField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ClientEntity_fk, identifier, jsonPath, shortName
            FROM ClientEntityMaterialField
            WHERE ClientEntityMaterialField_pk = ?
            ''',
            (self.ClientEntityMaterialField_pk,)
        ).fetchone()

    @property
    def ClientEntity_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]

    @property
    def shortName(self) -> str:
        return self.query_result()[3]


class ClientEntityAnimationField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.ClientEntityAnimationField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ClientEntity_fk, identifier, jsonPath, shortName
            FROM ClientEntityAnimationField
            WHERE ClientEntityAnimationField_pk = ?
            ''',
            (self.ClientEntityAnimationField_pk,)
        ).fetchone()

    @property
    def ClientEntity_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]

    @property
    def shortName(self) -> str:
        return self.query_result()[3]


class ClientEntityAnimationControllerField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.ClientEntityAnimationControllerField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ClientEntity_fk, identifier, jsonPath, shortName
            FROM ClientEntityAnimationControllerField
            WHERE ClientEntityAnimationControllerField_pk = ?
            ''',
            (self.ClientEntityAnimationControllerField_pk,)
        ).fetchone()

    @property
    def ClientEntity_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]

    @property
    def shortName(self) -> str:
        return self.query_result()[3]


class RenderControllerFile:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.RenderControllerFile_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ResourcePack_fk, path
            FROM RenderControllerFile
            WHERE RenderControllerFile_pk = ?
            ''',
            (self.RenderControllerFile_pk,)
        ).fetchone()

    @property
    def ResourcePack_fk(self) -> int | None:
        return self.query_result()[0]

    @property
    def path(self) -> Path:
        return self.query_result()[1]


class RenderController:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.RenderController_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT RenderControllerFile_fk, identifier, jsonPath
            FROM RenderController
            WHERE RenderController_pk = ?
            ''',
            (self.RenderController_pk,)
        ).fetchone()

    @property
    def RenderControllerFile_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]


class RenderControllerTexturesField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.RenderControllerTexturesField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT RenderController_fk, inOwnerArrayJsonPath, jsonPath, ownerArray, shortName
            FROM RenderControllerTexturesField
            WHERE RenderControllerTexturesField_pk = ?
            ''',
            (self.RenderControllerTexturesField_pk,)
        ).fetchone()

    @property
    def RenderController_fk(self) -> int:
        return self.query_result()[0]

    @property
    def inOwnerArrayJsonPath(self) -> str | None:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]

    @property
    def ownerArray(self) -> str | None:
        return self.query_result()[3]

    @property
    def shortName(self) -> str:
        return self.query_result()[4]


class RenderControllerMaterialsField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.RenderControllerMaterialsField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT RenderController_fk, boneNamePattern, inOwnerArrayJsonPath, jsonPath, ownerArray, shortName
            FROM RenderControllerMaterialsField
            WHERE RenderControllerMaterialsField_pk = ?
            ''',
            (self.RenderControllerMaterialsField_pk,)
        ).fetchone()

    @property
    def RenderController_fk(self) -> int:
        return self.query_result()[0]

    @property
    def boneNamePattern(self) -> str | None:
        return self.query_result()[1]

    @property
    def inOwnerArrayJsonPath(self) -> str | None:
        return self.query_result()[2]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[3]

    @property
    def ownerArray(self) -> str | None:
        return self.query_result()[4]

    @property
    def shortName(self) -> str:
        return self.query_result()[5]


class RenderControllerGeometryField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.RenderControllerGeometryField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT RenderController_fk, inOwnerArrayJsonPath, jsonPath, ownerArray, shortName
            FROM RenderControllerGeometryField
            WHERE RenderControllerGeometryField_pk = ?
            ''',
            (self.RenderControllerGeometryField_pk,)
        ).fetchone()

    @property
    def RenderController_fk(self) -> int:
        return self.query_result()[0]

    @property
    def inOwnerArrayJsonPath(self) -> str | None:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]

    @property
    def ownerArray(self) -> str | None:
        return self.query_result()[3]

    @property
    def shortName(self) -> str:
        return self.query_result()[4]


class GeometryFile:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.GeometryFile_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ResourcePack_fk, path
            FROM GeometryFile
            WHERE GeometryFile_pk = ?
            ''',
            (self.GeometryFile_pk,)
        ).fetchone()

    @property
    def ResourcePack_fk(self) -> int | None:
        return self.query_result()[0]

    @property
    def path(self) -> Path:
        return self.query_result()[1]


class Geometry:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.Geometry_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT GeometryFile_fk, identifier, jsonPath, parent
            FROM Geometry
            WHERE Geometry_pk = ?
            ''',
            (self.Geometry_pk,)
        ).fetchone()

    @property
    def GeometryFile_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]

    @property
    def parent(self) -> str | None:
        return self.query_result()[3]


class TextureFile:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.TextureFile_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ResourcePack_fk, identifier, path
            FROM TextureFile
            WHERE TextureFile_pk = ?
            ''',
            (self.TextureFile_pk,)
        ).fetchone()

    @property
    def ResourcePack_fk(self) -> int | None:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def path(self) -> Path:
        return self.query_result()[2]


class ParticleFile:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.ParticleFile_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ResourcePack_fk, path
            FROM ParticleFile
            WHERE ParticleFile_pk = ?
            ''',
            (self.ParticleFile_pk,)
        ).fetchone()

    @property
    def ResourcePack_fk(self) -> int | None:
        return self.query_result()[0]

    @property
    def path(self) -> Path:
        return self.query_result()[1]


class Particle:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.Particle_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ParticleFile_fk, identifier, material, texture
            FROM Particle
            WHERE Particle_pk = ?
            ''',
            (self.Particle_pk,)
        ).fetchone()

    @property
    def ParticleFile_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def material(self) -> str | None:
        return self.query_result()[2]

    @property
    def texture(self) -> str | None:
        return self.query_result()[3]


class RpAnimationFile:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.RpAnimationFile_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ResourcePack_fk, path
            FROM RpAnimationFile
            WHERE RpAnimationFile_pk = ?
            ''',
            (self.RpAnimationFile_pk,)
        ).fetchone()

    @property
    def ResourcePack_fk(self) -> int | None:
        return self.query_result()[0]

    @property
    def path(self) -> Path:
        return self.query_result()[1]


class RpAnimation:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.RpAnimation_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT RpAnimationFile_fk, identifier, jsonPath
            FROM RpAnimation
            WHERE RpAnimation_pk = ?
            ''',
            (self.RpAnimation_pk,)
        ).fetchone()

    @property
    def RpAnimationFile_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]


class RpAnimationParticleEffect:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.RpAnimationParticleEffect_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT RpAnimation_fk, jsonPath, shortName
            FROM RpAnimationParticleEffect
            WHERE RpAnimationParticleEffect_pk = ?
            ''',
            (self.RpAnimationParticleEffect_pk,)
        ).fetchone()

    @property
    def RpAnimation_fk(self) -> int:
        return self.query_result()[0]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[1]

    @property
    def shortName(self) -> str:
        return self.query_result()[2]


class RpAnimationSoundEffect:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.RpAnimationSoundEffect_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT RpAnimation_fk, jsonPath, shortName
            FROM RpAnimationSoundEffect
            WHERE RpAnimationSoundEffect_pk = ?
            ''',
            (self.RpAnimationSoundEffect_pk,)
        ).fetchone()

    @property
    def RpAnimation_fk(self) -> int:
        return self.query_result()[0]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[1]

    @property
    def shortName(self) -> str:
        return self.query_result()[2]


class RpAnimationControllerFile:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.RpAnimationControllerFile_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ResourcePack_fk, path
            FROM RpAnimationControllerFile
            WHERE RpAnimationControllerFile_pk = ?
            ''',
            (self.RpAnimationControllerFile_pk,)
        ).fetchone()

    @property
    def ResourcePack_fk(self) -> int | None:
        return self.query_result()[0]

    @property
    def path(self) -> Path:
        return self.query_result()[1]


class RpAnimationController:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.RpAnimationController_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT RpAnimationControllerFile_fk, identifier, jsonPath
            FROM RpAnimationController
            WHERE RpAnimationController_pk = ?
            ''',
            (self.RpAnimationController_pk,)
        ).fetchone()

    @property
    def RpAnimationControllerFile_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]


class RpAnimationControllerParticleEffect:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.RpAnimationControllerParticleEffect_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT RpAnimationController_fk, jsonPath, shortName
            FROM RpAnimationControllerParticleEffect
            WHERE RpAnimationControllerParticleEffect_pk = ?
            ''',
            (self.RpAnimationControllerParticleEffect_pk,)
        ).fetchone()

    @property
    def RpAnimationController_fk(self) -> int:
        return self.query_result()[0]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[1]

    @property
    def shortName(self) -> str:
        return self.query_result()[2]


class RpAnimationControllerSoundEffect:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.RpAnimationControllerSoundEffect_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT RpAnimationController_fk, jsonPath, shortName
            FROM RpAnimationControllerSoundEffect
            WHERE RpAnimationControllerSoundEffect_pk = ?
            ''',
            (self.RpAnimationControllerSoundEffect_pk,)
        ).fetchone()

    @property
    def RpAnimationController_fk(self) -> int:
        return self.query_result()[0]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[1]

    @property
    def shortName(self) -> str:
        return self.query_result()[2]


class AttachableFile:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.AttachableFile_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ResourcePack_fk, path
            FROM AttachableFile
            WHERE AttachableFile_pk = ?
            ''',
            (self.AttachableFile_pk,)
        ).fetchone()

    @property
    def ResourcePack_fk(self) -> int | None:
        return self.query_result()[0]

    @property
    def path(self) -> Path:
        return self.query_result()[1]


class Attachable:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.Attachable_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT AttachableFile_fk, identifier
            FROM Attachable
            WHERE Attachable_pk = ?
            ''',
            (self.Attachable_pk,)
        ).fetchone()

    @property
    def AttachableFile_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]


class AttachableItemField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.AttachableItemField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT Attachable_fk, condition, identifier, jsonPath
            FROM AttachableItemField
            WHERE AttachableItemField_pk = ?
            ''',
            (self.AttachableItemField_pk,)
        ).fetchone()

    @property
    def Attachable_fk(self) -> int:
        return self.query_result()[0]

    @property
    def condition(self) -> str | None:
        return self.query_result()[1]

    @property
    def identifier(self) -> str:
        return self.query_result()[2]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[3]


class AttachableMaterialField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.AttachableMaterialField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT Attachable_fk, identifier, jsonPath, shortName
            FROM AttachableMaterialField
            WHERE AttachableMaterialField_pk = ?
            ''',
            (self.AttachableMaterialField_pk,)
        ).fetchone()

    @property
    def Attachable_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]

    @property
    def shortName(self) -> str:
        return self.query_result()[3]


class AttachableTextureField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.AttachableTextureField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT Attachable_fk, identifier, jsonPath, shortName
            FROM AttachableTextureField
            WHERE AttachableTextureField_pk = ?
            ''',
            (self.AttachableTextureField_pk,)
        ).fetchone()

    @property
    def Attachable_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]

    @property
    def shortName(self) -> str:
        return self.query_result()[3]


class AttachableGeometryField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.AttachableGeometryField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT Attachable_fk, identifier, jsonPath, shortName
            FROM AttachableGeometryField
            WHERE AttachableGeometryField_pk = ?
            ''',
            (self.AttachableGeometryField_pk,)
        ).fetchone()

    @property
    def Attachable_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]

    @property
    def shortName(self) -> str:
        return self.query_result()[3]


class AttachableRenderControllerField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.AttachableRenderControllerField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT Attachable_fk, condition, identifier, jsonPath
            FROM AttachableRenderControllerField
            WHERE AttachableRenderControllerField_pk = ?
            ''',
            (self.AttachableRenderControllerField_pk,)
        ).fetchone()

    @property
    def Attachable_fk(self) -> int:
        return self.query_result()[0]

    @property
    def condition(self) -> str | None:
        return self.query_result()[1]

    @property
    def identifier(self) -> str:
        return self.query_result()[2]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[3]


class AttachableAnimationField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.AttachableAnimationField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT Attachable_fk, identifier, jsonPath, shortName
            FROM AttachableAnimationField
            WHERE AttachableAnimationField_pk = ?
            ''',
            (self.AttachableAnimationField_pk,)
        ).fetchone()

    @property
    def Attachable_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]

    @property
    def shortName(self) -> str:
        return self.query_result()[3]


class AttachableAnimationControllerField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.AttachableAnimationControllerField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT Attachable_fk, identifier, jsonPath, shortName
            FROM AttachableAnimationControllerField
            WHERE AttachableAnimationControllerField_pk = ?
            ''',
            (self.AttachableAnimationControllerField_pk,)
        ).fetchone()

    @property
    def Attachable_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]

    @property
    def shortName(self) -> str:
        return self.query_result()[3]


class SoundDefinitionsFile:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.SoundDefinitionsFile_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ResourcePack_fk, path
            FROM SoundDefinitionsFile
            WHERE SoundDefinitionsFile_pk = ?
            ''',
            (self.SoundDefinitionsFile_pk,)
        ).fetchone()

    @property
    def ResourcePack_fk(self) -> int | None:
        return self.query_result()[0]

    @property
    def path(self) -> Path:
        return self.query_result()[1]


class SoundDefinition:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.SoundDefinition_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT SoundDefinitionsFile_fk, identifier, jsonPath
            FROM SoundDefinition
            WHERE SoundDefinition_pk = ?
            ''',
            (self.SoundDefinition_pk,)
        ).fetchone()

    @property
    def SoundDefinitionsFile_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]


class SoundDefinitionSoundField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.SoundDefinitionSoundField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT SoundDefinition_fk, identifier, jsonPath
            FROM SoundDefinitionSoundField
            WHERE SoundDefinitionSoundField_pk = ?
            ''',
            (self.SoundDefinitionSoundField_pk,)
        ).fetchone()

    @property
    def SoundDefinition_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]


class SoundFile:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.SoundFile_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ResourcePack_fk, identifier, path
            FROM SoundFile
            WHERE SoundFile_pk = ?
            ''',
            (self.SoundFile_pk,)
        ).fetchone()

    @property
    def ResourcePack_fk(self) -> int | None:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def path(self) -> Path:
        return self.query_result()[2]


class RpItemFile:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.RpItemFile_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT ResourcePack_fk, path
            FROM RpItemFile
            WHERE RpItemFile_pk = ?
            ''',
            (self.RpItemFile_pk,)
        ).fetchone()

    @property
    def ResourcePack_fk(self) -> int | None:
        return self.query_result()[0]

    @property
    def path(self) -> Path:
        return self.query_result()[1]


class RpItem:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.RpItem_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT RpItemFile_fk, icon, identifier
            FROM RpItem
            WHERE RpItem_pk = ?
            ''',
            (self.RpItem_pk,)
        ).fetchone()

    @property
    def RpItemFile_fk(self) -> int:
        return self.query_result()[0]

    @property
    def icon(self) -> str | None:
        return self.query_result()[1]

    @property
    def identifier(self) -> str:
        return self.query_result()[2]


class BehaviorPack:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.BehaviorPack_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT path
            FROM BehaviorPack
            WHERE BehaviorPack_pk = ?
            ''',
            (self.BehaviorPack_pk,)
        ).fetchone()

    @property
    def path(self) -> Path:
        return self.query_result()[0]


class EntityFile:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.EntityFile_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT BehaviorPack_fk, path
            FROM EntityFile
            WHERE EntityFile_pk = ?
            ''',
            (self.EntityFile_pk,)
        ).fetchone()

    @property
    def BehaviorPack_fk(self) -> int | None:
        return self.query_result()[0]

    @property
    def path(self) -> Path:
        return self.query_result()[1]


class Entity:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.Entity_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT EntityFile_fk, identifier
            FROM Entity
            WHERE Entity_pk = ?
            ''',
            (self.Entity_pk,)
        ).fetchone()

    @property
    def EntityFile_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]


class EntityLootFieldComponentTypeEnum:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.value: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT 
            FROM EntityLootFieldComponentTypeEnum
            WHERE value = ?
            ''',
            (self.value,)
        ).fetchone()


class EntityLootField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.EntityLootField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT Entity_fk, componentType, identifier, jsonPath
            FROM EntityLootField
            WHERE EntityLootField_pk = ?
            ''',
            (self.EntityLootField_pk,)
        ).fetchone()

    @property
    def Entity_fk(self) -> int:
        return self.query_result()[0]

    @property
    def componentType(self) -> str:
        return self.query_result()[1]

    @property
    def identifier(self) -> str:
        return self.query_result()[2]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[3]


class EntityTradeFieldComponentTypeEnum:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.value: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT 
            FROM EntityTradeFieldComponentTypeEnum
            WHERE value = ?
            ''',
            (self.value,)
        ).fetchone()


class EntityTradeField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.EntityTradeField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT Entity_fk, componentType, identifier, jsonPath
            FROM EntityTradeField
            WHERE EntityTradeField_pk = ?
            ''',
            (self.EntityTradeField_pk,)
        ).fetchone()

    @property
    def Entity_fk(self) -> int:
        return self.query_result()[0]

    @property
    def componentType(self) -> str:
        return self.query_result()[1]

    @property
    def identifier(self) -> str:
        return self.query_result()[2]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[3]


class EntitySpawnEggField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.EntitySpawnEggField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT Entity_fk, identifier
            FROM EntitySpawnEggField
            WHERE EntitySpawnEggField_pk = ?
            ''',
            (self.EntitySpawnEggField_pk,)
        ).fetchone()

    @property
    def Entity_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]


class LootTableFile:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.LootTableFile_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT BehaviorPack_fk, path
            FROM LootTableFile
            WHERE LootTableFile_pk = ?
            ''',
            (self.LootTableFile_pk,)
        ).fetchone()

    @property
    def BehaviorPack_fk(self) -> int | None:
        return self.query_result()[0]

    @property
    def path(self) -> Path:
        return self.query_result()[1]


class LootTable:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.LootTable_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT LootTableFile_fk, identifier
            FROM LootTable
            WHERE LootTable_pk = ?
            ''',
            (self.LootTable_pk,)
        ).fetchone()

    @property
    def LootTableFile_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]


class LootTableItemField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.LootTableItemField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT LootTable_fk, identifier, jsonPath
            FROM LootTableItemField
            WHERE LootTableItemField_pk = ?
            ''',
            (self.LootTableItemField_pk,)
        ).fetchone()

    @property
    def LootTable_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]


class LootTableItemSpawnEggReferenceFieldConnectinTypeEnum:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.value: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT 
            FROM LootTableItemSpawnEggReferenceFieldConnectinTypeEnum
            WHERE value = ?
            ''',
            (self.value,)
        ).fetchone()


class LootTableItemSpawnEggReferenceField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.LootTableItemSpawnEggReferenceField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT LootTableItemField_fk, connectionType, entityIdentifier, jsonPath, spawnEggIdentifier
            FROM LootTableItemSpawnEggReferenceField
            WHERE LootTableItemSpawnEggReferenceField_pk = ?
            ''',
            (self.LootTableItemSpawnEggReferenceField_pk,)
        ).fetchone()

    @property
    def LootTableItemField_fk(self) -> int:
        return self.query_result()[0]

    @property
    def connectionType(self) -> str:
        return self.query_result()[1]

    @property
    def entityIdentifier(self) -> str:
        return self.query_result()[2]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[3]

    @property
    def spawnEggIdentifier(self) -> str:
        return self.query_result()[4]


class LootTableLootTableField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.LootTableLootTableField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT LootTable_fk, identifier, jsonPath
            FROM LootTableLootTableField
            WHERE LootTableLootTableField_pk = ?
            ''',
            (self.LootTableLootTableField_pk,)
        ).fetchone()

    @property
    def LootTable_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]


class TradeTableFile:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.TradeTableFile_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT BehaviorPack_fk, path
            FROM TradeTableFile
            WHERE TradeTableFile_pk = ?
            ''',
            (self.TradeTableFile_pk,)
        ).fetchone()

    @property
    def BehaviorPack_fk(self) -> int | None:
        return self.query_result()[0]

    @property
    def path(self) -> Path:
        return self.query_result()[1]


class TradeTable:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.TradeTable_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT TradeTableFile_fk, identifier
            FROM TradeTable
            WHERE TradeTable_pk = ?
            ''',
            (self.TradeTable_pk,)
        ).fetchone()

    @property
    def TradeTableFile_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]


class TradeTableItemField:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.TradeTableItemField_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT TradeTable_fk, dataValue, identifier, jsonPath
            FROM TradeTableItemField
            WHERE TradeTableItemField_pk = ?
            ''',
            (self.TradeTableItemField_pk,)
        ).fetchone()

    @property
    def TradeTable_fk(self) -> int:
        return self.query_result()[0]

    @property
    def dataValue(self) -> int | None:
        return self.query_result()[1]

    @property
    def identifier(self) -> str:
        return self.query_result()[2]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[3]


class BpAnimationFile:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.BpAnimationFile_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT BehaviorPack_fk, path
            FROM BpAnimationFile
            WHERE BpAnimationFile_pk = ?
            ''',
            (self.BpAnimationFile_pk,)
        ).fetchone()

    @property
    def BehaviorPack_fk(self) -> int | None:
        return self.query_result()[0]

    @property
    def path(self) -> Path:
        return self.query_result()[1]


class BpAnimation:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.BpAnimation_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT BpAnimationFile_fk, identifier, jsonPath
            FROM BpAnimation
            WHERE BpAnimation_pk = ?
            ''',
            (self.BpAnimation_pk,)
        ).fetchone()

    @property
    def BpAnimationFile_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]


class BpAnimationControllerFile:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.BpAnimationControllerFile_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT BehaviorPack_fk, path
            FROM BpAnimationControllerFile
            WHERE BpAnimationControllerFile_pk = ?
            ''',
            (self.BpAnimationControllerFile_pk,)
        ).fetchone()

    @property
    def BehaviorPack_fk(self) -> int | None:
        return self.query_result()[0]

    @property
    def path(self) -> Path:
        return self.query_result()[1]


class BpAnimationController:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.BpAnimationController_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT BpAnimationControllerFile_fk, identifier, jsonPath
            FROM BpAnimationController
            WHERE BpAnimationController_pk = ?
            ''',
            (self.BpAnimationController_pk,)
        ).fetchone()

    @property
    def BpAnimationControllerFile_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def jsonPath(self) -> str:
        return self.query_result()[2]


class BpItemFile:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.BpItemFile_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT BehaviorPack_fk, path
            FROM BpItemFile
            WHERE BpItemFile_pk = ?
            ''',
            (self.BpItemFile_pk,)
        ).fetchone()

    @property
    def BehaviorPack_fk(self) -> int | None:
        return self.query_result()[0]

    @property
    def path(self) -> Path:
        return self.query_result()[1]


class BpItemParserVersionEnum:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.value: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT 
            FROM BpItemParserVersionEnum
            WHERE value = ?
            ''',
            (self.value,)
        ).fetchone()


class BpItem:
    def __init__(self, db: Connection, id: int):
        self.db: Connection = db
        self.BpItem_pk: int = id

    @cache
    def query_result(self):
        return self.db.execute(
            '''
            SELECT BpItemFile_fk, identifier, parserVersion, texture
            FROM BpItem
            WHERE BpItem_pk = ?
            ''',
            (self.BpItem_pk,)
        ).fetchone()

    @property
    def BpItemFile_fk(self) -> int:
        return self.query_result()[0]

    @property
    def identifier(self) -> str:
        return self.query_result()[1]

    @property
    def parserVersion(self) -> str:
        return self.query_result()[2]

    @property
    def texture(self) -> str | None:
        return self.query_result()[3]


# Map strings to class names for easy access
WRAPPER_CLASSES = {
    'ResourcePack': ResourcePack,
    'ClientEntityFile': ClientEntityFile,
    'ClientEntity': ClientEntity,
    'ClientEntityRenderControllerField': ClientEntityRenderControllerField,
    'ClientEntityGeometryField': ClientEntityGeometryField,
    'ClientEntityTextureField': ClientEntityTextureField,
    'ClientEntityMaterialField': ClientEntityMaterialField,
    'ClientEntityAnimationField': ClientEntityAnimationField,
    'ClientEntityAnimationControllerField': ClientEntityAnimationControllerField,
    'RenderControllerFile': RenderControllerFile,
    'RenderController': RenderController,
    'RenderControllerTexturesField': RenderControllerTexturesField,
    'RenderControllerMaterialsField': RenderControllerMaterialsField,
    'RenderControllerGeometryField': RenderControllerGeometryField,
    'GeometryFile': GeometryFile,
    'Geometry': Geometry,
    'TextureFile': TextureFile,
    'ParticleFile': ParticleFile,
    'Particle': Particle,
    'RpAnimationFile': RpAnimationFile,
    'RpAnimation': RpAnimation,
    'RpAnimationParticleEffect': RpAnimationParticleEffect,
    'RpAnimationSoundEffect': RpAnimationSoundEffect,
    'RpAnimationControllerFile': RpAnimationControllerFile,
    'RpAnimationController': RpAnimationController,
    'RpAnimationControllerParticleEffect': RpAnimationControllerParticleEffect,
    'RpAnimationControllerSoundEffect': RpAnimationControllerSoundEffect,
    'AttachableFile': AttachableFile,
    'Attachable': Attachable,
    'AttachableItemField': AttachableItemField,
    'AttachableMaterialField': AttachableMaterialField,
    'AttachableTextureField': AttachableTextureField,
    'AttachableGeometryField': AttachableGeometryField,
    'AttachableRenderControllerField': AttachableRenderControllerField,
    'AttachableAnimationField': AttachableAnimationField,
    'AttachableAnimationControllerField': AttachableAnimationControllerField,
    'SoundDefinitionsFile': SoundDefinitionsFile,
    'SoundDefinition': SoundDefinition,
    'SoundDefinitionSoundField': SoundDefinitionSoundField,
    'SoundFile': SoundFile,
    'RpItemFile': RpItemFile,
    'RpItem': RpItem,
    'BehaviorPack': BehaviorPack,
    'EntityFile': EntityFile,
    'Entity': Entity,
    'EntityLootFieldComponentTypeEnum': EntityLootFieldComponentTypeEnum,
    'EntityLootField': EntityLootField,
    'EntityTradeFieldComponentTypeEnum': EntityTradeFieldComponentTypeEnum,
    'EntityTradeField': EntityTradeField,
    'EntitySpawnEggField': EntitySpawnEggField,
    'LootTableFile': LootTableFile,
    'LootTable': LootTable,
    'LootTableItemField': LootTableItemField,
    'LootTableItemSpawnEggReferenceFieldConnectinTypeEnum': LootTableItemSpawnEggReferenceFieldConnectinTypeEnum,
    'LootTableItemSpawnEggReferenceField': LootTableItemSpawnEggReferenceField,
    'LootTableLootTableField': LootTableLootTableField,
    'TradeTableFile': TradeTableFile,
    'TradeTable': TradeTable,
    'TradeTableItemField': TradeTableItemField,
    'BpAnimationFile': BpAnimationFile,
    'BpAnimation': BpAnimation,
    'BpAnimationControllerFile': BpAnimationControllerFile,
    'BpAnimationController': BpAnimationController,
    'BpItemFile': BpItemFile,
    'BpItemParserVersionEnum': BpItemParserVersionEnum,
    'BpItem': BpItem,
}
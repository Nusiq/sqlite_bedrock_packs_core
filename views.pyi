import pathlib
import sqlite3
from typing import Any, Callable # pyright: ignore[reportUnusedImport]
from ._views import AbstractDBView # pyright: ignore[reportUnusedImport]
# First bp and rp
from ._db_resource_pack import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_behavior_pack import *   # pyright: ignore[reportGeneralTypeIssues]
# Then other tables that rely on them
from ._db_attachable import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_bp_animation import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_bp_animation_controller import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_bp_block import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_bp_item import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_client_entity import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_entity import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_geometry import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_loot_table import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_particle import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_render_controller import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_rp_animation import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_rp_animation_controller import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_rp_item import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_sound import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_sound_definitions import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_texture import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_trade_table import *   # pyright: ignore[reportGeneralTypeIssues]
from ._db_terrain_texture import *   # pyright: ignore[reportGeneralTypeIssues]
class ResourcePack(AbstractDBView):
    path: pathlib.Path
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class BehaviorPack(AbstractDBView):
    path: pathlib.Path
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class AttachableFile(AbstractDBView):
    path: pathlib.Path
    ResourcePack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class Attachable(AbstractDBView):
    identifier: str
    AttachableFile_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class AttachableItemField(AbstractDBView):
    identifier: str
    condition: str
    jsonPath: str
    Attachable_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class AttachableMaterialField(AbstractDBView):
    shortName: str
    identifier: str
    jsonPath: str
    Attachable_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class AttachableTextureField(AbstractDBView):
    shortName: str
    identifier: str
    jsonPath: str
    Attachable_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class AttachableGeometryField(AbstractDBView):
    shortName: str
    identifier: str
    jsonPath: str
    Attachable_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class AttachableRenderControllerField(AbstractDBView):
    identifier: str
    condition: str
    jsonPath: str
    Attachable_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class AttachableAnimationField(AbstractDBView):
    shortName: str
    identifier: str
    jsonPath: str
    Attachable_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class AttachableAnimationControllerField(AbstractDBView):
    shortName: str
    identifier: str
    jsonPath: str
    Attachable_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class BpAnimationFile(AbstractDBView):
    path: pathlib.Path
    BehaviorPack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class BpAnimation(AbstractDBView):
    identifier: str
    jsonPath: str
    BpAnimationFile_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class BpAnimationControllerFile(AbstractDBView):
    path: pathlib.Path
    BehaviorPack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class BpAnimationController(AbstractDBView):
    identifier: str
    jsonPath: str
    BpAnimationControllerFile_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class BpBlockFile(AbstractDBView):
    path: pathlib.Path
    BehaviorPack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class BpBlock(AbstractDBView):
    identifier: str
    BpBlockFile_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class BpBlockLootField(AbstractDBView):
    identifier: str
    jsonPath: str
    BpBlock_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class BpBlockGeometryField(AbstractDBView):
    identifier: str
    jsonPath: str
    BpBlock_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class BpBlockMaterialInstancesField(AbstractDBView):
    jsonPath: str
    BpBlock_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class BpBlockMaterialInstancesFieldInstance(AbstractDBView):
    identifier: str
    jsonPath: str
    texture: str
    renderMethod: str
    BpBlockMaterialInstancesField_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class BpItemFile(AbstractDBView):
    path: pathlib.Path
    BehaviorPack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class BpItem(AbstractDBView):
    identifier: str
    texture: str
    BpItemFile_fk: int
    parserVersion: str
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class ClientEntityFile(AbstractDBView):
    path: pathlib.Path
    ResourcePack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class ClientEntity(AbstractDBView):
    identifier: str
    ClientEntityFile_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class ClientEntityRenderControllerField(AbstractDBView):
    identifier: str
    condition: str
    jsonPath: str
    ClientEntity_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class ClientEntityGeometryField(AbstractDBView):
    shortName: str
    identifier: str
    jsonPath: str
    ClientEntity_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class ClientEntityTextureField(AbstractDBView):
    shortName: str
    identifier: str
    jsonPath: str
    ClientEntity_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class ClientEntityMaterialField(AbstractDBView):
    shortName: str
    identifier: str
    jsonPath: str
    ClientEntity_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class ClientEntityAnimationField(AbstractDBView):
    shortName: str
    identifier: str
    jsonPath: str
    ClientEntity_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class ClientEntityAnimationControllerField(AbstractDBView):
    shortName: str
    identifier: str
    jsonPath: str
    ClientEntity_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class EntityFile(AbstractDBView):
    path: pathlib.Path
    BehaviorPack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class Entity(AbstractDBView):
    identifier: str
    EntityFile_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class EntityLootField(AbstractDBView):
    identifier: str
    jsonPath: str
    Entity_fk: int
    componentType: str
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class EntityTradeField(AbstractDBView):
    identifier: str
    jsonPath: str
    Entity_fk: int
    componentType: str
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class EntitySpawnEggField(AbstractDBView):
    identifier: str
    Entity_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class GeometryFile(AbstractDBView):
    path: pathlib.Path
    ResourcePack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class Geometry(AbstractDBView):
    identifier: str
    parent: str
    jsonPath: str
    GeometryFile_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class LootTableFile(AbstractDBView):
    path: pathlib.Path
    BehaviorPack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class LootTable(AbstractDBView):
    identifier: str
    LootTableFile_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class LootTableItemField(AbstractDBView):
    identifier: str
    jsonPath: str
    LootTable_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class LootTableItemSpawnEggReferenceField(AbstractDBView):
    entityIdentifier: str
    spawnEggIdentifier: str
    jsonPath: str
    LootTableItemField_fk: int
    connectionType: str
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class LootTableLootTableField(AbstractDBView):
    identifier: str
    jsonPath: str
    LootTable_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class ParticleFile(AbstractDBView):
    path: pathlib.Path
    ResourcePack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class Particle(AbstractDBView):
    identifier: str
    material: str
    texture: str
    ParticleFile_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class RenderControllerFile(AbstractDBView):
    path: pathlib.Path
    ResourcePack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class RenderController(AbstractDBView):
    identifier: str
    jsonPath: str
    RenderControllerFile_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class RenderControllerTexturesField(AbstractDBView):
    ownerArray: str
    inOwnerArrayJsonPath: str
    shortName: str
    jsonPath: str
    RenderController_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class RenderControllerMaterialsField(AbstractDBView):
    ownerArray: str
    inOwnerArrayJsonPath: str
    shortName: str
    jsonPath: str
    boneNamePattern: str
    RenderController_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class RenderControllerGeometryField(AbstractDBView):
    ownerArray: str
    inOwnerArrayJsonPath: str
    shortName: str
    jsonPath: str
    RenderController_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class RpAnimationFile(AbstractDBView):
    path: pathlib.Path
    ResourcePack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class RpAnimation(AbstractDBView):
    identifier: str
    jsonPath: str
    RpAnimationFile_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class RpAnimationParticleEffect(AbstractDBView):
    shortName: str
    jsonPath: str
    RpAnimation_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class RpAnimationSoundEffect(AbstractDBView):
    shortName: str
    jsonPath: str
    RpAnimation_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class RpAnimationControllerFile(AbstractDBView):
    path: pathlib.Path
    ResourcePack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class RpAnimationController(AbstractDBView):
    identifier: str
    jsonPath: str
    RpAnimationControllerFile_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class RpAnimationControllerParticleEffect(AbstractDBView):
    shortName: str
    jsonPath: str
    RpAnimationController_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class RpAnimationControllerSoundEffect(AbstractDBView):
    shortName: str
    jsonPath: str
    RpAnimationController_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class RpItemFile(AbstractDBView):
    path: pathlib.Path
    ResourcePack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class RpItem(AbstractDBView):
    identifier: str
    icon: str
    RpItemFile_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class SoundFile(AbstractDBView):
    path: pathlib.Path
    identifier: str
    ResourcePack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class SoundDefinitionsFile(AbstractDBView):
    path: pathlib.Path
    ResourcePack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class SoundDefinition(AbstractDBView):
    identifier: str
    jsonPath: str
    SoundDefinitionsFile_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class SoundDefinitionSoundField(AbstractDBView):
    identifier: str
    jsonPath: str
    SoundDefinition_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class TextureFile(AbstractDBView):
    path: pathlib.Path
    identifier: str
    ResourcePack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class TradeTableFile(AbstractDBView):
    path: pathlib.Path
    BehaviorPack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class TradeTable(AbstractDBView):
    identifier: str
    TradeTableFile_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class TradeTableItemField(AbstractDBView):
    identifier: str
    dataValue: int
    jsonPath: str
    TradeTable_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class TradeTableItemSpawnEggReferenceField(AbstractDBView):
    entityIdentifier: str
    spawnEggIdentifier: str
    jsonPath: str
    TradeTableItemField_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class TerrainTextureFile(AbstractDBView):
    path: pathlib.Path
    ResourcePack_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class TerrainTexture(AbstractDBView):
    identifier: str
    TerrainTextureFile_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
class TerrainTextureVariation(AbstractDBView):
    identifier: str
    jsonPath: pathlib.Path
    variantIndex: int
    variationIndex: int
    weight: int
    tintColor: str
    overlayColor: str
    TerrainTexture_fk: int
    id: int
    connection: sqlite3.Connection
    query_result: Callable[[], tuple[Any, ...]]
    build_script: str
"""
THIS FILE IS GENERATED. DO NOT EDIT IT MANUALLY.
"""
from __future__ import annotations
from typing import NamedTuple

class _TableConnection(NamedTuple):
    columns: tuple[str, str]
    is_pk: bool

RELATION_MAP = {
    "ClientEntity": {
        "Entity": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "ClientEntityFile": _TableConnection(
            columns=(('ClientEntityFile_fk', 'ClientEntityFile_pk')),
            is_pk=True
        ),
        "ClientEntityAnimationControllerField": _TableConnection(
            columns=(('ClientEntity_pk', 'ClientEntity_fk')),
            is_pk=True
        ),
        "ClientEntityAnimationField": _TableConnection(
            columns=(('ClientEntity_pk', 'ClientEntity_fk')),
            is_pk=True
        ),
        "ClientEntityGeometryField": _TableConnection(
            columns=(('ClientEntity_pk', 'ClientEntity_fk')),
            is_pk=True
        ),
        "ClientEntityMaterialField": _TableConnection(
            columns=(('ClientEntity_pk', 'ClientEntity_fk')),
            is_pk=True
        ),
        "ClientEntityRenderControllerField": _TableConnection(
            columns=(('ClientEntity_pk', 'ClientEntity_fk')),
            is_pk=True
        ),
        "ClientEntityTextureField": _TableConnection(
            columns=(('ClientEntity_pk', 'ClientEntity_fk')),
            is_pk=True
        ),
    },
    "ClientEntityRenderControllerField": {
        "RenderController": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "ClientEntity": _TableConnection(
            columns=(('ClientEntity_fk', 'ClientEntity_pk')),
            is_pk=True
        ),
    },
    "ClientEntityGeometryField": {
        "Geometry": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "ClientEntity": _TableConnection(
            columns=(('ClientEntity_fk', 'ClientEntity_pk')),
            is_pk=True
        ),
    },
    "ClientEntityTextureField": {
        "TextureFile": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "ClientEntity": _TableConnection(
            columns=(('ClientEntity_fk', 'ClientEntity_pk')),
            is_pk=True
        ),
    },
    "ClientEntityAnimationField": {
        "RpAnimation": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "ClientEntity": _TableConnection(
            columns=(('ClientEntity_fk', 'ClientEntity_pk')),
            is_pk=True
        ),
    },
    "ClientEntityAnimationControllerField": {
        "RpAnimationController": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "ClientEntity": _TableConnection(
            columns=(('ClientEntity_fk', 'ClientEntity_pk')),
            is_pk=True
        ),
    },
    "AttachableItemField": {
        "RpItem": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "BpItem": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "Attachable": _TableConnection(
            columns=(('Attachable_fk', 'Attachable_pk')),
            is_pk=True
        ),
    },
    "AttachableTextureField": {
        "TextureFile": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "Attachable": _TableConnection(
            columns=(('Attachable_fk', 'Attachable_pk')),
            is_pk=True
        ),
    },
    "AttachableGeometryField": {
        "Geometry": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "Attachable": _TableConnection(
            columns=(('Attachable_fk', 'Attachable_pk')),
            is_pk=True
        ),
    },
    "AttachableRenderControllerField": {
        "RenderController": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "Attachable": _TableConnection(
            columns=(('Attachable_fk', 'Attachable_pk')),
            is_pk=True
        ),
    },
    "AttachableAnimationField": {
        "RpAnimation": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "Attachable": _TableConnection(
            columns=(('Attachable_fk', 'Attachable_pk')),
            is_pk=True
        ),
    },
    "AttachableAnimationControllerField": {
        "RpAnimationController": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "Attachable": _TableConnection(
            columns=(('Attachable_fk', 'Attachable_pk')),
            is_pk=True
        ),
    },
    "EntityLootField": {
        "LootTable": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "EntityLootFieldComponentTypeEnum": _TableConnection(
            columns=(('componentType', 'value')),
            is_pk=True
        ),
        "Entity": _TableConnection(
            columns=(('Entity_fk', 'Entity_pk')),
            is_pk=True
        ),
    },
    "EntityTradeField": {
        "TradeTable": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "EntityTradeFieldComponentTypeEnum": _TableConnection(
            columns=(('componentType', 'value')),
            is_pk=True
        ),
        "Entity": _TableConnection(
            columns=(('Entity_fk', 'Entity_pk')),
            is_pk=True
        ),
    },
    "LootTableItemField": {
        "RpItem": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "BpItem": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "LootTable": _TableConnection(
            columns=(('LootTable_fk', 'LootTable_pk')),
            is_pk=True
        ),
        "LootTableItemSpawnEggReferenceField": _TableConnection(
            columns=(('LootTableItemField_pk', 'LootTableItemField_fk')),
            is_pk=True
        ),
    },
    "LootTableItemSpawnEggReferenceField": {
        "EntitySpawnEggField": _TableConnection(
            columns=(('spawnEggIdentifier', 'identifier')),
            is_pk=False
        ),
        "LootTableItemSpawnEggReferenceFieldConnectinTypeEnum": _TableConnection(
            columns=(('connectionType', 'value')),
            is_pk=True
        ),
        "LootTableItemField": _TableConnection(
            columns=(('LootTableItemField_fk', 'LootTableItemField_pk')),
            is_pk=True
        ),
    },
    "LootTableLootTableField": {
        "LootTable": _TableConnection(
            columns=(('LootTable_fk', 'LootTable_pk')),
            is_pk=True
        ),
    },
    "TradeTableItemField": {
        "RpItem": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "BpItem": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "TradeTable": _TableConnection(
            columns=(('TradeTable_fk', 'TradeTable_pk')),
            is_pk=True
        ),
    },
    "Particle": {
        "TextureFile": _TableConnection(
            columns=(('texture', 'identifier')),
            is_pk=False
        ),
        "ParticleFile": _TableConnection(
            columns=(('ParticleFile_fk', 'ParticleFile_pk')),
            is_pk=True
        ),
    },
    "SoundDefinitionSoundField": {
        "SoundFile": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "SoundDefinition": _TableConnection(
            columns=(('SoundDefinition_fk', 'SoundDefinition_pk')),
            is_pk=True
        ),
    },
    "Entity": {
        "ClientEntity": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "EntityFile": _TableConnection(
            columns=(('EntityFile_fk', 'EntityFile_pk')),
            is_pk=True
        ),
        "EntityLootField": _TableConnection(
            columns=(('Entity_pk', 'Entity_fk')),
            is_pk=True
        ),
        "EntitySpawnEggField": _TableConnection(
            columns=(('Entity_pk', 'Entity_fk')),
            is_pk=True
        ),
        "EntityTradeField": _TableConnection(
            columns=(('Entity_pk', 'Entity_fk')),
            is_pk=True
        ),
    },
    "RenderController": {
        "ClientEntityRenderControllerField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "AttachableRenderControllerField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "RenderControllerFile": _TableConnection(
            columns=(('RenderControllerFile_fk', 'RenderControllerFile_pk')),
            is_pk=True
        ),
        "RenderControllerGeometryField": _TableConnection(
            columns=(('RenderController_pk', 'RenderController_fk')),
            is_pk=True
        ),
        "RenderControllerMaterialsField": _TableConnection(
            columns=(('RenderController_pk', 'RenderController_fk')),
            is_pk=True
        ),
        "RenderControllerTexturesField": _TableConnection(
            columns=(('RenderController_pk', 'RenderController_fk')),
            is_pk=True
        ),
    },
    "Geometry": {
        "ClientEntityGeometryField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "AttachableGeometryField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "GeometryFile": _TableConnection(
            columns=(('GeometryFile_fk', 'GeometryFile_pk')),
            is_pk=True
        ),
    },
    "TextureFile": {
        "ClientEntityTextureField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "AttachableTextureField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "Particle": _TableConnection(
            columns=(('identifier', 'texture')),
            is_pk=False
        ),
        "ResourcePack": _TableConnection(
            columns=(('ResourcePack_fk', 'ResourcePack_pk')),
            is_pk=True
        ),
    },
    "RpAnimation": {
        "ClientEntityAnimationField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "AttachableAnimationField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "RpAnimationFile": _TableConnection(
            columns=(('RpAnimationFile_fk', 'RpAnimationFile_pk')),
            is_pk=True
        ),
        "RpAnimationParticleEffect": _TableConnection(
            columns=(('RpAnimation_pk', 'RpAnimation_fk')),
            is_pk=True
        ),
        "RpAnimationSoundEffect": _TableConnection(
            columns=(('RpAnimation_pk', 'RpAnimation_fk')),
            is_pk=True
        ),
    },
    "RpAnimationController": {
        "ClientEntityAnimationControllerField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "AttachableAnimationControllerField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "RpAnimationControllerFile": _TableConnection(
            columns=(('RpAnimationControllerFile_fk', 'RpAnimationControllerFile_pk')),
            is_pk=True
        ),
        "RpAnimationControllerParticleEffect": _TableConnection(
            columns=(('RpAnimationController_pk', 'RpAnimationController_fk')),
            is_pk=True
        ),
        "RpAnimationControllerSoundEffect": _TableConnection(
            columns=(('RpAnimationController_pk', 'RpAnimationController_fk')),
            is_pk=True
        ),
    },
    "RpItem": {
        "AttachableItemField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "LootTableItemField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "TradeTableItemField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "RpItemFile": _TableConnection(
            columns=(('RpItemFile_fk', 'RpItemFile_pk')),
            is_pk=True
        ),
    },
    "BpItem": {
        "AttachableItemField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "LootTableItemField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "TradeTableItemField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "BpItemParserVersionEnum": _TableConnection(
            columns=(('parserVersion', 'value')),
            is_pk=True
        ),
        "BpItemFile": _TableConnection(
            columns=(('BpItemFile_fk', 'BpItemFile_pk')),
            is_pk=True
        ),
    },
    "LootTable": {
        "EntityLootField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "LootTableLootTableField": _TableConnection(
            columns=(('LootTable_pk', 'LootTable_fk')),
            is_pk=True
        ),
        "LootTableFile": _TableConnection(
            columns=(('LootTableFile_fk', 'LootTableFile_pk')),
            is_pk=True
        ),
        "LootTableItemField": _TableConnection(
            columns=(('LootTable_pk', 'LootTable_fk')),
            is_pk=True
        ),
    },
    "TradeTable": {
        "EntityTradeField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "TradeTableFile": _TableConnection(
            columns=(('TradeTableFile_fk', 'TradeTableFile_pk')),
            is_pk=True
        ),
        "TradeTableItemField": _TableConnection(
            columns=(('TradeTable_pk', 'TradeTable_fk')),
            is_pk=True
        ),
    },
    "EntitySpawnEggField": {
        "LootTableItemSpawnEggReferenceField": _TableConnection(
            columns=(('identifier', 'spawnEggIdentifier')),
            is_pk=False
        ),
        "Entity": _TableConnection(
            columns=(('Entity_fk', 'Entity_pk')),
            is_pk=True
        ),
    },
    "SoundFile": {
        "SoundDefinitionSoundField": _TableConnection(
            columns=(('identifier', 'identifier')),
            is_pk=False
        ),
        "ResourcePack": _TableConnection(
            columns=(('ResourcePack_fk', 'ResourcePack_pk')),
            is_pk=True
        ),
    },
    "AttachableFile": {
        "Attachable": _TableConnection(
            columns=(('AttachableFile_pk', 'AttachableFile_fk')),
            is_pk=True
        ),
        "ResourcePack": _TableConnection(
            columns=(('ResourcePack_fk', 'ResourcePack_pk')),
            is_pk=True
        ),
    },
    "Attachable": {
        "AttachableFile": _TableConnection(
            columns=(('AttachableFile_fk', 'AttachableFile_pk')),
            is_pk=True
        ),
        "AttachableAnimationControllerField": _TableConnection(
            columns=(('Attachable_pk', 'Attachable_fk')),
            is_pk=True
        ),
        "AttachableAnimationField": _TableConnection(
            columns=(('Attachable_pk', 'Attachable_fk')),
            is_pk=True
        ),
        "AttachableGeometryField": _TableConnection(
            columns=(('Attachable_pk', 'Attachable_fk')),
            is_pk=True
        ),
        "AttachableItemField": _TableConnection(
            columns=(('Attachable_pk', 'Attachable_fk')),
            is_pk=True
        ),
        "AttachableMaterialField": _TableConnection(
            columns=(('Attachable_pk', 'Attachable_fk')),
            is_pk=True
        ),
        "AttachableRenderControllerField": _TableConnection(
            columns=(('Attachable_pk', 'Attachable_fk')),
            is_pk=True
        ),
        "AttachableTextureField": _TableConnection(
            columns=(('Attachable_pk', 'Attachable_fk')),
            is_pk=True
        ),
    },
    "ResourcePack": {
        "AttachableFile": _TableConnection(
            columns=(('ResourcePack_pk', 'ResourcePack_fk')),
            is_pk=True
        ),
        "ClientEntityFile": _TableConnection(
            columns=(('ResourcePack_pk', 'ResourcePack_fk')),
            is_pk=True
        ),
        "GeometryFile": _TableConnection(
            columns=(('ResourcePack_pk', 'ResourcePack_fk')),
            is_pk=True
        ),
        "ParticleFile": _TableConnection(
            columns=(('ResourcePack_pk', 'ResourcePack_fk')),
            is_pk=True
        ),
        "RenderControllerFile": _TableConnection(
            columns=(('ResourcePack_pk', 'ResourcePack_fk')),
            is_pk=True
        ),
        "RpAnimationControllerFile": _TableConnection(
            columns=(('ResourcePack_pk', 'ResourcePack_fk')),
            is_pk=True
        ),
        "RpAnimationFile": _TableConnection(
            columns=(('ResourcePack_pk', 'ResourcePack_fk')),
            is_pk=True
        ),
        "RpItemFile": _TableConnection(
            columns=(('ResourcePack_pk', 'ResourcePack_fk')),
            is_pk=True
        ),
        "SoundDefinitionsFile": _TableConnection(
            columns=(('ResourcePack_pk', 'ResourcePack_fk')),
            is_pk=True
        ),
        "SoundFile": _TableConnection(
            columns=(('ResourcePack_pk', 'ResourcePack_fk')),
            is_pk=True
        ),
        "TextureFile": _TableConnection(
            columns=(('ResourcePack_pk', 'ResourcePack_fk')),
            is_pk=True
        ),
    },
    "AttachableMaterialField": {
        "Attachable": _TableConnection(
            columns=(('Attachable_fk', 'Attachable_pk')),
            is_pk=True
        ),
    },
    "BpAnimationFile": {
        "BpAnimation": _TableConnection(
            columns=(('BpAnimationFile_pk', 'BpAnimationFile_fk')),
            is_pk=True
        ),
        "BehaviorPack": _TableConnection(
            columns=(('BehaviorPack_fk', 'BehaviorPack_pk')),
            is_pk=True
        ),
    },
    "BpAnimation": {
        "BpAnimationFile": _TableConnection(
            columns=(('BpAnimationFile_fk', 'BpAnimationFile_pk')),
            is_pk=True
        ),
    },
    "BpAnimationControllerFile": {
        "BpAnimationController": _TableConnection(
            columns=(('BpAnimationControllerFile_pk', 'BpAnimationControllerFile_fk')),
            is_pk=True
        ),
        "BehaviorPack": _TableConnection(
            columns=(('BehaviorPack_fk', 'BehaviorPack_pk')),
            is_pk=True
        ),
    },
    "BpAnimationController": {
        "BpAnimationControllerFile": _TableConnection(
            columns=(('BpAnimationControllerFile_fk', 'BpAnimationControllerFile_pk')),
            is_pk=True
        ),
    },
    "BehaviorPack": {
        "BpAnimationControllerFile": _TableConnection(
            columns=(('BehaviorPack_pk', 'BehaviorPack_fk')),
            is_pk=True
        ),
        "BpAnimationFile": _TableConnection(
            columns=(('BehaviorPack_pk', 'BehaviorPack_fk')),
            is_pk=True
        ),
        "BpItemFile": _TableConnection(
            columns=(('BehaviorPack_pk', 'BehaviorPack_fk')),
            is_pk=True
        ),
        "EntityFile": _TableConnection(
            columns=(('BehaviorPack_pk', 'BehaviorPack_fk')),
            is_pk=True
        ),
        "LootTableFile": _TableConnection(
            columns=(('BehaviorPack_pk', 'BehaviorPack_fk')),
            is_pk=True
        ),
        "TradeTableFile": _TableConnection(
            columns=(('BehaviorPack_pk', 'BehaviorPack_fk')),
            is_pk=True
        ),
    },
    "BpItemParserVersionEnum": {
        "BpItem": _TableConnection(
            columns=(('value', 'parserVersion')),
            is_pk=True
        ),
    },
    "BpItemFile": {
        "BpItem": _TableConnection(
            columns=(('BpItemFile_pk', 'BpItemFile_fk')),
            is_pk=True
        ),
        "BehaviorPack": _TableConnection(
            columns=(('BehaviorPack_fk', 'BehaviorPack_pk')),
            is_pk=True
        ),
    },
    "ClientEntityFile": {
        "ClientEntity": _TableConnection(
            columns=(('ClientEntityFile_pk', 'ClientEntityFile_fk')),
            is_pk=True
        ),
        "ResourcePack": _TableConnection(
            columns=(('ResourcePack_fk', 'ResourcePack_pk')),
            is_pk=True
        ),
    },
    "ClientEntityMaterialField": {
        "ClientEntity": _TableConnection(
            columns=(('ClientEntity_fk', 'ClientEntity_pk')),
            is_pk=True
        ),
    },
    "EntityFile": {
        "Entity": _TableConnection(
            columns=(('EntityFile_pk', 'EntityFile_fk')),
            is_pk=True
        ),
        "BehaviorPack": _TableConnection(
            columns=(('BehaviorPack_fk', 'BehaviorPack_pk')),
            is_pk=True
        ),
    },
    "EntityLootFieldComponentTypeEnum": {
        "EntityLootField": _TableConnection(
            columns=(('value', 'componentType')),
            is_pk=True
        ),
    },
    "EntityTradeFieldComponentTypeEnum": {
        "EntityTradeField": _TableConnection(
            columns=(('value', 'componentType')),
            is_pk=True
        ),
    },
    "GeometryFile": {
        "Geometry": _TableConnection(
            columns=(('GeometryFile_pk', 'GeometryFile_fk')),
            is_pk=True
        ),
        "ResourcePack": _TableConnection(
            columns=(('ResourcePack_fk', 'ResourcePack_pk')),
            is_pk=True
        ),
    },
    "LootTableFile": {
        "LootTable": _TableConnection(
            columns=(('LootTableFile_pk', 'LootTableFile_fk')),
            is_pk=True
        ),
        "BehaviorPack": _TableConnection(
            columns=(('BehaviorPack_fk', 'BehaviorPack_pk')),
            is_pk=True
        ),
    },
    "LootTableItemSpawnEggReferenceFieldConnectinTypeEnum": {
        "LootTableItemSpawnEggReferenceField": _TableConnection(
            columns=(('value', 'connectionType')),
            is_pk=True
        ),
    },
    "ParticleFile": {
        "Particle": _TableConnection(
            columns=(('ParticleFile_pk', 'ParticleFile_fk')),
            is_pk=True
        ),
        "ResourcePack": _TableConnection(
            columns=(('ResourcePack_fk', 'ResourcePack_pk')),
            is_pk=True
        ),
    },
    "RenderControllerFile": {
        "RenderController": _TableConnection(
            columns=(('RenderControllerFile_pk', 'RenderControllerFile_fk')),
            is_pk=True
        ),
        "ResourcePack": _TableConnection(
            columns=(('ResourcePack_fk', 'ResourcePack_pk')),
            is_pk=True
        ),
    },
    "RenderControllerGeometryField": {
        "RenderController": _TableConnection(
            columns=(('RenderController_fk', 'RenderController_pk')),
            is_pk=True
        ),
    },
    "RenderControllerMaterialsField": {
        "RenderController": _TableConnection(
            columns=(('RenderController_fk', 'RenderController_pk')),
            is_pk=True
        ),
    },
    "RenderControllerTexturesField": {
        "RenderController": _TableConnection(
            columns=(('RenderController_fk', 'RenderController_pk')),
            is_pk=True
        ),
    },
    "RpAnimationFile": {
        "RpAnimation": _TableConnection(
            columns=(('RpAnimationFile_pk', 'RpAnimationFile_fk')),
            is_pk=True
        ),
        "ResourcePack": _TableConnection(
            columns=(('ResourcePack_fk', 'ResourcePack_pk')),
            is_pk=True
        ),
    },
    "RpAnimationControllerFile": {
        "RpAnimationController": _TableConnection(
            columns=(('RpAnimationControllerFile_pk', 'RpAnimationControllerFile_fk')),
            is_pk=True
        ),
        "ResourcePack": _TableConnection(
            columns=(('ResourcePack_fk', 'ResourcePack_pk')),
            is_pk=True
        ),
    },
    "RpAnimationControllerParticleEffect": {
        "RpAnimationController": _TableConnection(
            columns=(('RpAnimationController_fk', 'RpAnimationController_pk')),
            is_pk=True
        ),
    },
    "RpAnimationControllerSoundEffect": {
        "RpAnimationController": _TableConnection(
            columns=(('RpAnimationController_fk', 'RpAnimationController_pk')),
            is_pk=True
        ),
    },
    "RpAnimationParticleEffect": {
        "RpAnimation": _TableConnection(
            columns=(('RpAnimation_fk', 'RpAnimation_pk')),
            is_pk=True
        ),
    },
    "RpAnimationSoundEffect": {
        "RpAnimation": _TableConnection(
            columns=(('RpAnimation_fk', 'RpAnimation_pk')),
            is_pk=True
        ),
    },
    "RpItemFile": {
        "RpItem": _TableConnection(
            columns=(('RpItemFile_pk', 'RpItemFile_fk')),
            is_pk=True
        ),
        "ResourcePack": _TableConnection(
            columns=(('ResourcePack_fk', 'ResourcePack_pk')),
            is_pk=True
        ),
    },
    "SoundDefinitionsFile": {
        "SoundDefinition": _TableConnection(
            columns=(('SoundDefinitionsFile_pk', 'SoundDefinitionsFile_fk')),
            is_pk=True
        ),
        "ResourcePack": _TableConnection(
            columns=(('ResourcePack_fk', 'ResourcePack_pk')),
            is_pk=True
        ),
    },
    "SoundDefinition": {
        "SoundDefinitionsFile": _TableConnection(
            columns=(('SoundDefinitionsFile_fk', 'SoundDefinitionsFile_pk')),
            is_pk=True
        ),
        "SoundDefinitionSoundField": _TableConnection(
            columns=(('SoundDefinition_pk', 'SoundDefinition_fk')),
            is_pk=True
        ),
    },
    "TradeTableFile": {
        "TradeTable": _TableConnection(
            columns=(('TradeTableFile_pk', 'TradeTableFile_fk')),
            is_pk=True
        ),
        "BehaviorPack": _TableConnection(
            columns=(('BehaviorPack_fk', 'BehaviorPack_pk')),
            is_pk=True
        ),
    },
}
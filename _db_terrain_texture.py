# pylint: disable=no-member, multiple-statements, missing-module-docstring, missing-class-docstring
from typing import cast, Optional, NamedTuple, Iterator, TYPE_CHECKING
from sqlite3 import Connection
from pathlib import Path
import json
from .better_json_tools import load_jsonc, JSONWalker
from ._views import dbtableview, WeakTableConnection


@dbtableview(
    properties={
        "path": (Path, "NOT NULL")
    },
    connects_to=["ResourcePack"]
)
class TerrainTextureFile: ...

@dbtableview(
    properties={
        "identifier": (str, "NOT NULL")
    },
    connects_to=["TerrainTextureFile"]
)
class TerrainTexture: ...

@dbtableview(
    properties={
        # A path to the texture file without the extension
        "identifier": (str, "NOT NULL"),
        "jsonPath": (Path, "NOT NULL"),
        "variantIndex": (int, ""),  # For convinience (could be deduced from the path)
        "variationIndex": (int, ""),  # For convinience (could be deduced from the path)
        "weight": (int, ""),  # Only makes sense if the texture is a variation
        "tintColor": (str, ""),
        "overlayColor": (str, ""),
    },
    connects_to=["TerrainTexture"],
    weak_connects_to=[
        WeakTableConnection("identifier", "TextureFile", "identifier")
    ]
)
class TerrainTextureVariation:
    '''
    Represents a singl file path mentioned in the terrain_texture.json file.

    It is called "variation" because in the most deeply nested configuration
    of the terrain_texture.json file the textures are listed as multiple
    variations. If the TerrainTexture doesn't use variations property, then
    this class will represent its main texture. Note that the terrain textures
    not only have variations, but also can have multiple variants that can
    optionally use the variations. Here is a list of all possible JSON paths
    to a texture path (the item at the end of the JSON path is as string with
    the path to the texture file):

    - root.textures.<identifier>.textures
    - root.textures.<identifier>.textures[<variant_index>]
    - root.textures.<identifier>.textures[<variant_index>].path
    - root.textures.<identifier>.textures[<variant_index>].
        variations[variation_index]
    - root.textures.<identifier>.textures[<variant_index>].
        variations[variation_index].path
    '''

TERRAIN_TEXTURE_BUILD_SCRIPT: str = (
    TerrainTextureFile.build_script +
    TerrainTexture.build_script +
    TerrainTextureVariation.build_script
)

def load_terrain_texture(db: Connection, rp_id: int):
    '''
    Loads the terrain_texture.json file from the resource pack.
    '''
    rp_path: Path = db.execute(
        "SELECT path FROM ResourcePack WHERE ResourcePack_pk = ?",
        (rp_id,)
    ).fetchone()[0]

    terrain_texture_path = rp_path / "textures" / "terrain_texture.json"
    if not terrain_texture_path.exists():
        return
    load_terrain_texture_items(db, terrain_texture_path, rp_id)

def load_terrain_texture_items(db: Connection, terrain_texture_path: Path, rp_id: int):
    '''
    Loads all terrain textures from the terrain_texture.json file.
    '''
    cursor = db.cursor()
    # TERRAIN TEXTURE FILE
    cursor.execute(
        "INSERT INTO TerrainTextureFile (path, ResourcePack_fk) VALUES (?, ?)",
        (terrain_texture_path.as_posix(), rp_id)
    )
    file_pk = cursor.lastrowid
    try:
        terrain_texture_json = load_jsonc(terrain_texture_path)
    except json.JSONDecodeError:
        return

    # We will inset the TerrainTexture only if it has at least one valid
    # variation. This function reduces the boilerplate code.
    inserted_textures: dict[str, int] = {}
    def _insert_terrain_texture_variation(
            terrain_texture_identifier: str,
            identifier: str,
            json_path: str,
            variant_index: Optional[int] = None,
            variation_index: Optional[int] = None,
            weight: Optional[int] = None,
            tint_color: Optional[str] = None,
            overlay_color: Optional[str] = None) -> None:
        '''
        Inserts a TerrainTextureVariation into the database and connects it
        to specified TerrainTexture. If the TerrainTexture doesn't exist,
        it will be created.
        '''
        # Get or create the TerrainTexture PK
        if terrain_texture_identifier in inserted_textures:
            terrain_texture_pk = inserted_textures[terrain_texture_identifier]
        else:
            cursor.execute(
                "INSERT INTO TerrainTexture "
                "(identifier, TerrainTextureFile_fk) VALUES (?, ?)",
                (terrain_texture_identifier, file_pk)
            )
            terrain_texture_pk = cursor.lastrowid
            terrain_texture_pk = cast(int, terrain_texture_pk)
            inserted_textures[terrain_texture_identifier] = terrain_texture_pk

        cursor.execute(
            "INSERT INTO TerrainTextureVariation "
            "(identifier, jsonPath, variantIndex, variationIndex, weight, "
            "tintColor, overlayColor, TerrainTexture_fk) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                identifier,
                json_path,
                variant_index,
                variation_index,
                weight,
                tint_color,
                overlay_color,
                terrain_texture_pk
            )
        )

    for terrain_texture_walker in terrain_texture_json / "texture_data" // str:
        identifier = terrain_texture_walker.parent_key
        identifier = cast(str, identifier)
        textures_walker = terrain_texture_walker / "textures"
        if isinstance(textures_walker.data, str):
            _insert_terrain_texture_variation(
                terrain_texture_identifier=identifier,
                identifier=textures_walker.data,
                json_path=textures_walker.path_str)
        elif isinstance(textures_walker.data, list):
            for variant_walker, variant_index, variant_data in _yield_variants(
                    textures_walker):
                if variant_data.is_variation:
                    if TYPE_CHECKING:  # We know that from is_variation.
                        assert isinstance(variant_data.path, str)
                        assert isinstance(variant_data.json_path, str)
                    _insert_terrain_texture_variation(
                       terrain_texture_identifier=identifier,
                       identifier=variant_data.path,
                       json_path=variant_data.json_path,
                       variant_index=variant_index,
                       tint_color=variant_data.tint_color,
                       overlay_color=variant_data.overlay_color)
                    continue
                # variant_data.is_variation == False
                for variation_index, variation_data in _yield_variations(
                            variant_walker):
                    _insert_terrain_texture_variation(
                        terrain_texture_identifier=identifier,
                        identifier=variation_data.identifier,
                        json_path=variation_data.json_path,
                        variant_index=variant_index,
                        variation_index=variation_index,
                        weight=variation_data.weight,
                        tint_color=variant_data.tint_color,
                        overlay_color=variant_data.overlay_color
                    )
        elif isinstance(textures_walker.data, dict):
            try:
                variant_data = _get_variant_data(textures_walker)
            except ValueError:
                return
            if variant_data.is_variation:
                if TYPE_CHECKING:  # We know that from is_variation.
                    assert isinstance(variant_data.path, str)
                    assert isinstance(variant_data.json_path, str)
                _insert_terrain_texture_variation(
                    terrain_texture_identifier=identifier,
                    identifier=variant_data.path,
                    json_path=variant_data.json_path,
                    tint_color=variant_data.tint_color,
                    overlay_color=variant_data.overlay_color)
            else:
                for variation_index, variation_data in _yield_variations(
                        textures_walker):
                    _insert_terrain_texture_variation(
                        terrain_texture_identifier=identifier,
                        identifier=variation_data.identifier,
                        json_path=variation_data.json_path,
                        variation_index=variation_index,
                        weight=variation_data.weight,
                        tint_color=variant_data.tint_color,
                        overlay_color=variant_data.overlay_color
                    )
        else:
            return # Skip silently

# Following functions are used to remove massive amount of boilerplate code
# that would be required to parse all possible JSON paths to variations and
# variants.
class _VariationData(NamedTuple):
    identifier: str
    weight: Optional[int]
    json_path: str

def _get_variation_data(variation_walker: JSONWalker) -> _VariationData:
    '''
    Returns the identifier and weight of the variation from the JSONWalker.
    '''
    # The data is a string
    if isinstance(variation_walker.data, str):
        return _VariationData(
            variation_walker.data, None, variation_walker.path_str)
    # The data is a dict
    identifier_walker = variation_walker / "path"
    if not identifier_walker.exists:
        raise ValueError("Variation doesn't have a path")
    if not isinstance(identifier_walker.data, str):
        raise ValueError("Variation path is not a string")
    identifier = identifier_walker.data
    weight_walker = variation_walker / "weight"
    weight = None
    if weight_walker.exists and isinstance(weight_walker.data, int):
        weight = weight_walker.data
    return _VariationData(identifier, weight, identifier_walker.path_str)

class _VariantData(NamedTuple):
    path: Optional[str]
    json_path: Optional[str]
    tint_color: Optional[str]
    overlay_color: Optional[str]

    @property
    def is_variation(self) -> bool:
        '''
        Whether this variant is a variation or variations are nested inside.
        '''
        return self.path is not None

def _yield_variations(variant_walker: JSONWalker) -> Iterator[tuple[int, _VariationData]]:
    '''
    Yields the variation data from the JSONWalker that represents a variant.
    '''
    for variation_walker in variant_walker / "variations" // int:
        variation_index = variation_walker.parent_key
        variation_index = cast(int, variation_index)
        try:
            variation_data = _get_variation_data(variation_walker)
        except ValueError:
            continue
        yield variation_index, variation_data

def _get_variant_data(variant_walker: JSONWalker) -> _VariantData:
    '''
    Returns the path, overlay_color, tint_color and json_path of the variant
    from the JSONWalker.
    '''
    # The data is a string
    if isinstance(variant_walker.data, str):
        return _VariantData(
            variant_walker.data, variant_walker.path_str, None, None)
    # The data is a dict
    if not isinstance(variant_walker.data, dict):
        raise ValueError("Variant is not a string or a dict")
    path_walker = variant_walker / "path"
    path = None
    json_path = None
    if path_walker.exists and isinstance(path_walker.data, str):
        path = path_walker.data
        json_path = path_walker.path_str
    overlay_color_walker = variant_walker / "overlay_color"
    overlay_color = None
    if overlay_color_walker.exists and isinstance(overlay_color_walker.data, str):
        overlay_color = overlay_color_walker.data
    tint_color_walker = variant_walker / "tint_color"
    tint_color = None
    if tint_color_walker.exists and isinstance(tint_color_walker.data, str):
        tint_color = tint_color_walker.data
    return _VariantData(
        path, json_path, tint_color, overlay_color)

def _yield_variants(
    variant_list_walker: JSONWalker
) -> Iterator[tuple[JSONWalker, int, _VariantData]]:
    '''
    Yields the variant data from the JSONWalker that represents a variant list.
    '''
    for variant_walker in variant_list_walker // int:
        variant_index = variant_walker.parent_key
        variant_index = cast(int, variant_index)
        try:
            variant_data = _get_variant_data(variant_walker)
        except ValueError:
            continue
        yield variant_walker, variant_index, variant_data

'''
Functions for analyzing Molang expressions.
'''
import re
from collections import defaultdict

def find_molang_resources(molang: str, resource_prefixes: list[str]):
    '''
    Finds all resources of specified types in a molang expression. Returns
    a dictionary keyed by the resource type, with values being a list of
    found resources.

    Example
    -------
    >>> find_molang_resources(
            molang=(
                "q.is_baby = 1 ? array.skin[q.variant] : ("
                "q.mark_variant ? texture.default : texture.default2)"
            ),
            resource_prefixes=["array", "texture"])
    {'array': ['array.skin'], 'texture': ['texture.default', 'texture.default2']}
    '''
    molang = molang.lower()
    results: dict[str, list] = {prefix: [] for prefix in resource_prefixes}
    for resource_prefix in resource_prefixes:
        # Capturing groups crops the first part of the search for example:
        # geometry.default -> geometry
        item_pattern = re.compile(
            f'{resource_prefix}\\.(\\w+)', flags=re.IGNORECASE)
        for item_name in item_pattern.findall(molang):
            results[resource_prefix].append(item_name)
    return results

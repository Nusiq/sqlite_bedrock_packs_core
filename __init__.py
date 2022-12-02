# Weird "import X as X" syntax is for some IDEs to recognize the imports
# as values accessible from the module.
from .db_main import (
    Database as Database
)
from .easy_query import (
    EasyQuery as EasyQuery,
    Left as Left,
)

VERSION = (2, 1, 1)  # COMPATIBILITY BREAK, NEW FEATURE, BUGFIX
__version__ = '.'.join([str(x) for x in VERSION])

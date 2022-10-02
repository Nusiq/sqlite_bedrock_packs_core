from .db_main import create_db, load_rp, open_db

VERSION = (1, 2, 0)  # COMPATIBILITY BREAK, NEW FEATURE, BUGFIX
__version__ = '.'.join([str(x) for x in VERSION])

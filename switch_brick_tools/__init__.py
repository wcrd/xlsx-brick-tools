# import submodules to use if needed
from . import generator, validator
# import specifics to make life easier
from .generator import Dataset, Graph
from .validator import validate

import logging

logging.basicConfig(
    level=logging.INFO
)
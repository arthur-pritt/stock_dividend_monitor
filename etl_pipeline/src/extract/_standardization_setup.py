from dataclasses import dataclass
from edgar.xbrl.standardization import(
    get_default_mapper,
    get_default_store,
    StandardConcept,
    StandardizationCache
    
)

@dataclass

class StandardizationContext:
    mapper : object 
    store : object
    cache : object


def build_standardization_context():
    """ 
    A function that initializes and returns three prepared edgar object
    """
    ctx= StandardizationContext(
        mapper= get_default_mapper(),
        store= get_default_store(),
        cache =StandardizationCache())
    return ctx

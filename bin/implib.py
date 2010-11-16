"""
Import the library rooted above this file's directory
"""


def _implib(name):
    """ Import the library rooted above this file's directory as the given name
        Args:
            name - the name to import the library as
    """
    import sys
    import os
    import imp

    ps = os.path.split

    module_path, module_name = \
        ps(ps(ps(__file__)[0])[0])

    sys.modules[name] = \
        imp.load_module(name, *imp.find_module(module_name, [module_path]))


_implib('libcchdo')

def _abs_import(name='libcchdo'):
    import sys
    import os
    import imp
    import __builtin__

    s = sys.path
    p = os.path

    module_path, module_name = \
        p.split(p.join(os.sep, *s[0].split(os.sep)[:-1]))

    sys.modules[name] = \
        imp.load_module(name, *imp.find_module(module_name, [module_path]))


_abs_import()

import sys
import os
import imp
import __builtin__


_NAME = 'libcchdo'


_s = sys.path
_p = os.path


_module_path, _module_name = _p.split(_p.join(os.sep,
                                      *_s[0].split(os.sep)[:-1]))
# XXX HACK - Insert library directly into global namespace.
__builtin__.__dict__[_NAME] = \
    imp.load_module(_NAME, *imp.find_module(_module_name, [_module_path]))

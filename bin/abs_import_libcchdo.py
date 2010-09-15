import sys
import os
import imp


LIBRARY_NAME = 'libcchdo'


_s = sys.path
_p = os.path


_module_path, _module_name = _p.split(_p.join(os.sep, *_s[0].split(os.sep)[:-1]))
imp.load_module(LIBRARY_NAME, *imp.find_module(_module_name, [_module_path]))

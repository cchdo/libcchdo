"""Libcchdo utilities."""
import os
import __builtin__
import functools


from StringIO import StringIO as pyStringIO
try:
    from cStringIO import StringIO
except ImportError:
    StringIO = pyStringIO


class memoize(object):
    '''Decorator. Caches a function's return value each time it is called.
    If called later with the same arguments, the cached value is returned 
    (not reevaluated).

    Grabbed from http://wiki.python.org/moin/PythonDecoratorLibrary#Memoize 
    2012-04-20

    '''
    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, *args):
        try:
            return self.cache[args]
        except KeyError:
            value = self.func(*args)
            self.cache[args] = value
            return value
        except TypeError:
            # uncachable -- for instance, passing a list as an argument.
            # Better to not cache than to blow up entirely.
            return self.func(*args)

    def __repr__(self):
        '''Return the function's docstring.'''
        return self.func.__doc__

    def __get__(self, obj, objtype):
        '''Support instance methods.'''
        return functools.partial(self.__call__, obj)


@memoize
def get_library_abspath():
    """Give the absolute path of the directory that is the root of the 
       package, i.e. it contains this file.
    """
    return os.path.split(os.path.realpath(__file__))[0]

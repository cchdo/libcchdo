import os


def sample_file(*args):
    return os.path.join(os.path.dirname(__file__), 'samples', *args)

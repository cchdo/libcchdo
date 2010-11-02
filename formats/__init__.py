

_pre_write_functions = []


def pre_write(self):
    """ Should be called by all writers before doing anything. """
    for fn in _pre_write_functions:
    	fn(self)


def add_pre_write(fn):
    _pre_write_functions.append(fn)


def _report_changes(self):
    if self.changes_to_report:
        self.globals['header'] = '\n'.join(
            map(lambda x: '#' + x, self.changes_to_report +
                                   [self.globals['stamp']]) + 
            [self.globals['header']])


add_pre_write(_report_changes)

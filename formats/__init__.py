

def pre_write(self):
    """ Should be called by all writers before doing anything. """
    if self.changes_to_report:
        self.globals['header'] = '\n'.join(
            map(lambda x: '#' + x, self.changes_to_report +
                                   [self.globals['stamp']]) + 
            [self.globals['header']])

#!/usr/bin/env python

from __future__ import with_statement
import sys
sys.path.insert(0, '/'.join(sys.path[0].split('/')[:-2]))

import libcchdo
import libcchdo.db.parameters
import libcchdo.db.model
import libcchdo.db.model.legacy
import libcchdo.db.model.std

def main():
    legacy_parameters = map(
         lambda x: x.name,
         libcchdo.db.model.legacy.session().query(
             libcchdo.db.model.legacy.Parameter).all())

    std_parameters = map(
        lambda x: libcchdo.db.parameters.find_by_mnemonic(x),
        legacy_parameters)

    libcchdo.db.model.std.create_all()
    std_session = libcchdo.db.model.std.session()
    std_session.add_all(std_parameters)
    std_session.commit()
    std_session.close()

    return 0


if __name__ == '__main__':
    sys.exit(main())

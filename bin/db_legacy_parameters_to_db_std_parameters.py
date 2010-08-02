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
    legacy_parameters = [x[0] for x in 
        libcchdo.db.model.legacy.session().query(
            libcchdo.db.model.legacy.Parameter.name).all()]

    std_parameters = [libcchdo.db.parameters.find_by_mnemonic(x) for x in 
        legacy_parameters]

    # Additional modifications
    std_parameters.insert(0, libcchdo.db.model.std.Parameter(
        'EXPOCODE', 'ExpoCode', '%11s', display_order=1))
    std_parameters.insert(1, libcchdo.db.model.std.Parameter(
        'SECT_ID', 'Section ID', '%11s', display_order=2))

    libcchdo.db.model.std.create_all()

    std_session = libcchdo.db.model.std.session()
    std_session.add_all(std_parameters)
    std_session.commit()
    std_session.close()

    return 0


if __name__ == '__main__':
    sys.exit(main())

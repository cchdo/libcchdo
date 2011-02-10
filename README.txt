========
libcchdo
========

This library provides a format-less data model for the CCHDO and a centralized
place to put ways to read and write from it. Said format-less data model is
based on DataFiles which have Columns that are associated with Parameters. From
the data model, the user may write out to a database model of the data or read
in more data and mash it together or write it out in a different format. When
it is said the data is format-less, it is actually in a neutral format that
lets it be manipulated easily into other formats.

Package Structure
=================

* libcchdo
  * bin - Utility scripts
  * libcchdo - library source files
  * scripts - a collection of scripts (yes I'm a pack-rat)

Testing
=======

Every file under tests/ will be run as a python test battery:

        python setup.py test

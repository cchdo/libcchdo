========
libcchdo
========

Utilities to manage CCHDO data.

Package Structure
=================

* libcchdo
  * bin - Utility scripts
  * libcchdo - library source files
  * scripts - a collection of scripts (yes I'm a pack-rat)

Testing
=======

Every file under tests/ will be run as a python test battery::

        python setup.py test


Quick start
===========

Reading a file
--------------

For example, a bottle exchange file 'bottle_hy1.csv'.

libcchdo attempts to abstract files into a DataFile object. Let's create one to
hold the data in 'bottle_hy1.csv'.

    >>> import libcchdo.model.datafile as DF
    >>> import libcchdo.formats.bottle.exchange as botex

    >>> d = DF.DataFile()

    >>> f = open('bottle_hy1.csv')
    >>> botex.read(d, f)
    # Stuff will be logged here about the file
    >>> f.close()

    # Let's explore the DataFile a little.
    >>> d.columns

    # Pretend that 'bottle_hy1.csv' has a column for OXYGEN, then
    >>> d.columns['OXYGEN'].parameter


Do some changes to the file. For example, let's delete OXYGEN from the file.

    >>> del d.columns['OXYGEN']

Now to write the masked file back out
    
    >>> output = open('masked_bottle_hy1.csv', 'w')
    >>> botex.write(d, output)
    >>> output.close()

Using some binaries

    $ path/to/installation/any_to_type --help
    $ path/to/installation/any_to_type --type nav test_hy1.csv



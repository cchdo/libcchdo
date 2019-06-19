# `hydro` command and docker

This library has been "dockerized" to help with the complex task of
getting all the dependencies installed, including what amounts to a copy
of the old cchdo mysql database. While installing this is now as easy
as: `docker pull cchdo/libcchdo` actually invoking the `hydro` command
is a little tricky.

## To install

First get docker for your platform, on a mac it is best to do a `brew
cask install docker` to avoid all the "you must login" on docker hub
stuff.

Then do a `docker pull cchdo/libcchdo`, a bunch of progress bars will
appear and when it's all done that's it\!

## To run

To start try `docker run -it --rm cchdo/libcchdo`, you should see the
help text print and the final words "hydro: error: too few arguments".
This means things are working. The `--rm` flag in this context means
"clean up the container after it has run". The `-it` flags mean
"interactive" and "tty", these are needed for keyboard interaction if
needed.

Next a mapping of the file system needs to occur so that libcchdo has
access to the data you want to work on. The "docker" way of doing this
is mapping entire directories, done with the `-v` flag. The
containerization was tested during development using `-v
$(cwd):/context` and this is what I recommend.

`docker run -it --rm -v $(cwd):/context cchdo/libcchdo`

will run but just output the same message as before (basically, nothing
to do). The `-v` flag is basically `map source:destination`, `/context`
is a directory inside the docker container where it will always run
from.

There is one more thing to do and that is make some envvars for the
"merger" identity used for creating the "stamp" of exchange files. This
is done with the following envvars:

  - LIBCCHDO\_MERGER\_DIVISION
  - LIBCCHDO\_MERGER\_INSTITUTION
  - LIBCCHDO\_MERGER\_INITIALS

These are passed into the docker run command using the `-e` flag (which
can be chained), so now the entire thing looks like this:

`docker run -it --rm -e LIBCCHDO_MERGER_DIVISION=CCH -e LIBCCHDO_MERGER_INITIALS=AMB -e LIBCCHDO_MERGER_INSTITUTION=SIO -v $(pwd):/context cchdo/libcchdo`

I'd highly recommend aliasing that entire thing (with the correct
initials) to `hydro<` in your shell. Then
check to make sure everything looks as it should with a
`hydro env` which should output
`prod` followed by an email address and the
stamp it will write to exchange files.

## Limitations

*  File paths are must be relative and deeper than whatever directory
   was mapped to `/context` inside the container. Docker usually doesn't
   allow the mapping of `/` of the host to
   anything for security reasons. This means any scripts which called
   "hydro" using absolute paths should be updated.

# libcchdo

Utilities to manage CCHDO data.

## Package Structure

  - libcchdo
      - libcchdo - library source files (see docstrings for more
        information)
      - scripts - a collection of scripts (yes I'm a pack-rat)

## Testing

Every file under tests/ will be run as a python test battery:

    $ python setup.py test

## Quick start

### Reading a file

For example, a bottle exchange file
<span class="title-ref">bottle\_hy1.csv</span>.

libcchdo attempts to abstract files into a DataFile object. Let's create
one to hold the data in <span class="title-ref">bottle\_hy1.csv</span>.

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

Do some changes to the file. For example, let's delete OXYGEN from the
file.

    >>> del d.columns['OXYGEN']

Now, write the masked file back out

    >>> output = open('masked_bottle_hy1.csv', 'w')
    >>> botex.write(d, output)
    >>> output.close()

Using some binaries

    $ path/to/installation/hydro --help
    $ path/to/installation/hydro convet --help
    $ path/to/installation/hydro convert any_to_type --type nav test_hy1.csv

# Pinba2
An attempt to rethink internal implementation and some features of excellent https://github.com/tony2001/pinba_engine by @tony2001.

# Building
requires
- gcc 4.9.4+ (but will increase this requirement to something like gcc6+ soon)
- boost: http://boost.org/
- meow: https://github.com/anton-povarov/meow
- nanomsg: http://nanomsg.org/
- google sparsehash: https://github.com/sparsehash/sparsehash

To build, run

    $ ./buildconf.sh

    $ ./configure
        --prefix=<path>
        --with-mysql=<path to mysql source code (you *MUST* have run configure in there first)>
        --with-nanomsg=<nanomsg install dir>
        --with-sparsehash=<path>
        --with-meow=<path>
        --with-boost=<path (need headers only)>

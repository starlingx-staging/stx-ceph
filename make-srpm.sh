#!/bin/sh

#
# Create a SRPM which can be used to build Ceph
#

vers=`echo 'stx-ceph_v13.2.0' | sed 's/^stx-ceph_v//'`
./make-dist $ver
rpmbuild -D"_sourcedir `pwd`" -D"_specdir `pwd`" -D"_srcrpmdir `pwd`" -bs ceph.spec

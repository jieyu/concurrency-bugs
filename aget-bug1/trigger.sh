#!/bin/sh

i=1
while [ $i -ne 0 ]; do
	./aget-devel/aget -n2 -l tmp.file http://mirror.candidhosting.com/pub/apache/httpd/httpd-2.2.17.tar.gz
        rm -f tmp.file
	sleep 0.1s
done

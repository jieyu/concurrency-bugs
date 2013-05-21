# Makefile used to compile runtran.cc

MYSQL_HOME=/opt/bugs/mysql-4.1.1/mysql-4.1.1-alpha

CXX = g++
CXXFLAGS = -g3 -O2 -Wshadow -Wall
LDFLAGS = -L$(MYSQL_HOME)/lib/mysql -Wl,-R$(MYSQL_HOME)/lib/mysql -I$(MYSQL_HOME)/include -lpthread -lmysqlclient -lz

all: runtran

runtran: runtran.cc
	${CXX} $(CXXFLAGS) -o runtran $< $(LDFLAGS)

clean:
	rm -f runtran

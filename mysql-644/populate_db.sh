#~/bin/sh

MYSQL_HOME=/opt/bugs/mysql-4.1.1/mysql-4.1.1-alpha

DB=test
PASSWD=
HOST=localhost
MYSQL=$MYSQL_HOME/bin/mysql
#MYSQL=/usr/local/mysql/bin/mysql

CMD="./runtran --repeat --seed 65323445 --database $DB --trace trace.txt --monitor pinot"

TIME="30 360 1"

BNAME="results"

if true; then
	# prepare table a & b
	$MYSQL -u root -D $DB -e 'drop table if exists a'
	$MYSQL -u root -D $DB -e 'drop table if exists b'
	echo  $MYSQL -u root -D $DB -e "drop table if exists b"

	# populate table a
	$MYSQL -u root -D $DB -e 'create table a (id int auto_increment not null primary key, tal int not null default 1)';
	rm -f /tmp/t
	for i in `seq 1 9`; do
		echo "insert into a set id=null, tal=$i" >> /tmp/t
		echo '\g' >> /tmp/t
	done;
	$MYSQL -u root -D $DB < /tmp/t
    
    # populate table b
	$MYSQL -u root -D $DB -e 'create table b (id int auto_increment not null primary key, tal int not null default 1);'
	rm -f /tmp/t
	for i in `seq 1 9`; do
		echo "insert into b set id=null, tal=$(( $i * $i ))" >> /tmp/t
		echo '\g' >> /tmp/t
	done;
	$MYSQL -u root -D $DB < /tmp/t
	$MYSQL -u root -D $DB -e 'SELECT * FROM a'
	$MYSQL -u root -D $DB -e 'SELECT * FROM b'
fi

# generate query trace file
if true; then
	rm -f trace.txt
	for i in `seq 1 5000`; do
		echo "00:00:00,000 $DB B $i" >> trace.txt
		# means a random integer will be used in prepared query
		echo "00:00:00,000 $DB S $i select * from a,b where a.tal+b.tal=?" >> trace.txt
		echo "00:00:00,000 $DB C $i" >> trace.txt
	done;
fi;

# drive the DB on a single host directly
#$CMD --thread 1 --host pinot $TIME  ${BNAME}


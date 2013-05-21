create database testdb;
create table testdb.nameage(name VARCHAR(20), age INT);
create table testdb.idnameage(id MEDIUMINT NOT NULL AUTO_INCREMENT, name VARCHAR(20), age INT, primary key(id));

insert into testdb.nameage values ("aaa", 10);
insert into testdb.idnameage values (1, "bbb", 20);

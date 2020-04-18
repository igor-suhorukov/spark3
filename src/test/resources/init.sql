create table t1(
  id bigint auto_increment primary key,
  name varchar(255));

insert into t1
select x, 'Hello ' || x from system_range(0, 1000000);
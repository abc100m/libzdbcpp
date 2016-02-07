#select count(1) from test_1;

#drop table test_1;

create table test_1(id int, remark varchar(200));

insert into test_1  select 1, 'good' from dual;
insert into test_1  select 1, 'good' from dual;
insert into test_1  select 1, 'good' from dual;
insert into test_1  select 1, 'good' from dual;
insert into test_1  select 1, 'good' from dual;
insert into test_1  select 1, 'good' from dual;
insert into test_1  select 1, 'good' from dual;
insert into test_1  select 1, 'good' from dual;
insert into test_1  select 1, 'good' from dual;
insert into test_1  select 1, 'good' from dual;
insert into test_1  select 1, 'good' from dual;
insert into test_1  select 1, 'good' from dual;

insert into test_1 
  select 1, 'good'
	from test_1 a, test_1 b;
    
insert into test_1 
  select 1, 'good'
	from test_1 a, test_1 b;
    
commit;


    
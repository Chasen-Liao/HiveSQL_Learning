use learn_hive;

--  创建表
create table test(
    id int,
    name string,
    gender string
);

show tables;

-- 数据类型
-- string 特殊



-- 表类型：内部表和外部表
create  table stu(
    id int,
    name string
);

insert into stu values (1, '周杰伦'), (2, '林俊杰');

select *
from stu;

desc formatted stu;





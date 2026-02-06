-- Join操作
use learn_hive;

select * from emp limit 3;

select * from dept limit 3;
-- 有共同的字段deptno
select * from emp, dept where emp.deptno = dept.deptno; -- 笛卡尔积
select * from emp join dept on emp.deptno = dept.deptno; -- 内连接
-- 这两个为什么是一样的？
-- 内连接：两个表有相同的字段，并且字段的值相同
-- 外连接：两个表有相同的字段，但是字段的值可能不同
select loc, count(*)
from emp join dept d on emp.deptno = d.deptno group by loc;

-- 等值链接 & 不等值链接
-- 字段值相等就连接为一行
-- 不等值连接就是除了=的其他运算符


-- join类型
-- 内连接：inner join
-- inner join返回所有能join的行
select * from emp inner join dept on emp.deptno = dept.deptno;
-- 左外连接：left outer join
-- 返回所有左表行，如果右表有匹配的行，则返回匹配行，否则返回NULL
-- dept是右表
select * from emp left join dept on emp.deptno = dept.deptno;

-- 右外连接：right outer join
-- 返回所有右表行，如果左表有匹配的行，则返回匹配行，否则返回NULL
select * from emp right join dept on emp.deptno = dept.deptno; -- 右表是dept

-- 全外连接：full outer join -- 满外连接
-- 返回所有行，如果两个表有匹配的行，则返回匹配行，否则返回NULL
select * from emp full join dept on emp.deptno = dept.deptno;


-- join 多表连接
-- 在创一个location表
create table location(
    loc int, -- 部门编号
    loc_name string -- 部门位置
) row format delimited fields terminated by '\t';

load data local inpath '/home/hadoop/data/hive/location.txt' into table location;

-- 多表连接
select
    *
from emp
join dept d on emp.deptno = d.deptno
join location l on d.loc = l.loc; -- join 后面是跟着要连接的右表

-- 联合union & union all
-- 纵向拼接（上下）
-- 注意：
-- 1. 两个表必须字段完全相同
-- 2. 列的顺序必须相同
-- 3. union去重，union all不去重
-- 列的个数和类型要相同
select * from emp where sal > 3000
union
select * from emp where sal < 1000;
select * from emp where sal = 800
union
select * from emp;
-- 只能链接select语句，不能链接其他语句
-- 字段名字按照第一个union的select语句为准



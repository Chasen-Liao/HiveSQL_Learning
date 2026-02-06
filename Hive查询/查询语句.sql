-- 前置建表
use learn_hive;

-- 数据准备

-- 建表
-- 部门表
create table if not exists dept(
    deptno int comment '部门编号',
    dname string comment '部门名字',
    loc int
) row format delimited fields terminated by '\t';

-- 员工表
create table if not exists emp(
    empno int comment '员工编号',
    ename string comment '员工名字',
    job string,
    sal double comment '员工薪资',
    deptno int comment '部门编号'
) row format delimited fields terminated by '\t';

-- 从Linux本地load数据到HDFS
load data local inpath '/home/hadoop/data/hive/dept.txt' into table dept;
load data local inpath '/home/hadoop/data/hive/emp.txt' into table emp;

-- 全表查询
select *
from dept;
select *
from emp;

-- 特定查询
select empno, ename
from emp;

-- limit
select empno, ename
from emp limit 2, 3;

-- where
select count(job)
from emp
where job = '研发';

-- group by
select job, count(job)
from emp
group by job;

-- having
select job, count(job)
from emp
group by job
having count(job) > 2;

-- order by
select empno, ename
from emp
order by empno desc;

-- join
select emp.empno, emp.ename, dept.dname
from emp, dept
where emp.deptno = dept.deptno
order by emp.empno desc ;

-- 子查询
select empno, ename
from emp
where sal > (select avg(sal) from emp); -- sal 大于平均水平的员工







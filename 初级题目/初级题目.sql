-- 题目
use learn_hive;

-- 建立表

-- 创建学生表
DROP TABLE IF EXISTS student;
create table if not exists student_info(
    stu_id string COMMENT '学生id',
    stu_name string COMMENT '学生姓名',
    birthday string COMMENT '出生日期',
    sex string COMMENT '性别'
)
row format delimited fields terminated by ','
stored as textfile;

-- 创建课程表
DROP TABLE IF EXISTS course;
create table if not exists course_info(
    course_id string COMMENT '课程id',
    course_name string COMMENT '课程名',
    tea_id string COMMENT '任课老师id'
)
row format delimited fields terminated by ','
stored as textfile;

-- 创建老师表
DROP TABLE IF EXISTS teacher;
create table if not exists teacher_info(
    tea_id string COMMENT '老师id',
    tea_name string COMMENT '学生姓名'
)
row format delimited fields terminated by ','
stored as textfile;

-- 创建分数表
DROP TABLE IF EXISTS score;
create table if not exists score_info(
    stu_id string COMMENT '学生id',
    course_id string COMMENT '课程id',
    score int COMMENT '成绩'
)
row format delimited fields terminated by ','
stored as textfile;

-- 插入数据
load data local inpath '/home/hadoop/data/hive/student_info.txt' into table student_info;
load data local inpath '/home/hadoop/data/hive/course_info.txt' into table course_info;
load data local inpath '/home/hadoop/data/hive/teacher_info.txt' into table teacher_info;
load data local inpath '/home/hadoop/data/hive/score_info.txt' into table score_info;

-- 查询
select * from student_info limit 3;

-- 2.1.2 查询姓“王”老师的个数, 返回名字
select count(*) as num, tea_name from teacher_info
where tea_name like '王%' group by tea_name;

-- 2.1.3 检索课程编号为“04”且分数小于60的学生的课程信息，结果按分数降序排列
select * from score_info
where course_id = '04' and score < 60 order by score desc;

-- 2.1.4 查询数学成绩不及格的学生和其对应的成绩，按照学号升序排序
select *
from student_info, score_info where score_info.stu_id = student_info.stu_id;

-- select stu_name, score
-- from score_info, student_info
-- where score_info.stu_id = student_info.stu_id
-- and course_name = '数学' and score < 60 order by stu_id; -- 链接操作之后coures_name 字段会丢失
select s.stu_id, s.stu_name, t1.score
from student_info as s join (
    select *
    from score_info
    where course_id = (select course_id from course_info where course_name='数学') and score < 60
    ) t1 on s.stu_id = t1.stu_id order by s.stu_id asc;

-- 3.1.1 查询编号为“02”的课程的总成绩和学生名字和id
select course_id, sum(score) as sum_score
from score_info
where course_id = '02'
group by course_id;
-- 3.3.1 查询平均成绩大于60分的学生的学号和平均成绩
select stu_id, avg(score) as avg_score
from score_info
group by stu_id
having avg_score > 60;



-- 3.4.2 按照如下格式显示学生的语文、数学、英语三科成绩，没有成绩的输出为0，按照学生的有效平均成绩降序显示
select si.stu_id,
       sum(if(ci.course_name = '语文', score, 0)) `语文`,
       sum(if(ci.course_name = '数学', score, 0)) `数学`,
       sum(if(ci.course_name = '英语', score, 0)) `英语`,
       count(*)                                 `有效课程数`,
       avg(si.score)                            `平均成绩`
from score_info si
         join
     course_info ci
     on
         si.course_id = ci.course_id
group by si.stu_id -- 非聚合字段
order by `平均成绩` desc;


-- 3.4.3 查询一共参加三门课程且其中一门为语文课程的学生的id和姓名

select t2.stu_id, t3.stu_name
from (select t1.stu_id
      from (
               select stu_id,
                      course_id
               from score_info
               where stu_id in
                     (
                         select stu_id
                         from score_info
                         where course_id = (select course_id from course_info where course_name = '语文')
                     )
           ) t1
      group by t1.stu_id
      having count(*) = 3) t2
         join student_info t3
              on t2.stu_id = t3.stu_id;

-- 同等写法
-- 先看要哪些表，然后就join操作，先把基本的虚拟表构建出来后再来写查询
select score_info.stu_id, si.stu_name
from score_info
         join student_info si on score_info.stu_id = si.stu_id -- 有了学生姓名,以为score_info里面没有名字, 所以要连接
group by score_info.stu_id, si.stu_name
having count(score_info.course_id) = 3
   and sum(if(score_info.course_id = '01', 1, 0)) >= 1;

-- 查询至少参加了两门课程，且有选修“英语”（course_id = '03'） 的学生 ID 和姓名。
select sc.stu_id, si.stu_name
from score_info sc
         join student_info si on sc.stu_id = si.stu_id
group by sc.stu_id, si.stu_name
having count(sc.course_id) >= 2
   and sum(if(sc.course_id = '03', 1, 0)) = 1;
-- sum(if(sc.course_id = (select course_id from course_info where course_name='英语') , 1, 0)) = 0;
-- having 里面不能用where!

-- 三表连接，把所有要用的字段所在的表都连接起来
select sc.stu_id, si.stu_name
from score_info sc
         join student_info si on sc.stu_id = si.stu_id
         join course_info ci on sc.course_id = ci.course_id
group by sc.stu_id, si.stu_name
having count(sc.course_id) >= 2
   and sum(if(ci.course_name = '英语', 1, 0)) >= 1;
-- having 跟where一样，都是过滤，但是where是在数据源上过滤，having是在分组上过滤
-- 先把所有要用的字段所在的表都join之后就可以直接使用这些字段名写条件


use learn_hive;
-- MR设置Yarn模式
set mapreduce.framework.name=yarn;


-- 查看所有函数
show functions;

-- 查看某个函数的详细描述
describe function extended length;

-- 数值函数
-- 1.round() 四舍五入函数
desc function extended round;
select round(12.3456, 2) as result;
-- ceil 函数: 向上取整
-- ceil是天花板的意思
desc function extended ceil;
select ceil(12.3456) as result;
-- floor 函数: 向下取整
desc function extended floor;


select floor(12.3456) as result;
-- 字符串函数
-- substring
desc function extended substring;
select substring('Faceboooook', 5, 2) as result;
select substring('Faceboooook', -2, 2) as result;
-- 注意！下标从1开始

-- replace
desc function extended replace; -- 把所有符合的字符串替换成新的字符串
select replace('Faceboooook', 'oo', 'o') as result;
-- 正则替换
describe function extended regexp_replace;
select regexp_replace('100-300', '(\\d+)', 'lcz') result; -- 需要转义字符
desc function extended regexp;

select 'fb' regexp 'f*';
-- repeat 重复字符串
desc function extended repeat;
--
select repeat('lcz', 3) as result;
-- split字符串切割
desc function extended split;
-- 还支持用正则表达式切割
select split('lcz,lcz.lcz', '[,.]') as result;
-- 返回值是数组array
-- nvl 函数: 如果为null则返回指定的值
desc function extended nvl;
--
select nvl(null, 'lcz') as result;
select nvl('l', null) as result;
-- concat函数: 连接字符串
desc function extended concat;
--
select concat('lcz', '-', 'lcz') as result;
-- concat_ws
desc function extended concat_ws;
select concat_ws('-', array('lcz', 'hhy')) as result;
-- get_json_object
desc function extended get_json_object;
-- $是Root Object ; .是子选择器 ; [0]是数组选择器
select get_json_object('[{"name":"大海海","sex":"男","age":"25"},{"name":"小宋宋","sex":"男","age":"47"}]', '$.[0].name');

--
select get_json_object('[{"name":"大海海","sex":"男","age":"25"},{"name":"小宋宋","sex":"男","age":"47"}]', '$.[0]');

-- 时间函数
-- current_timestamp
select `current_timestamp`() as result;
-- datediff
desc function extended datediff;
-- 获取两个时间间隔的天数
select datediff('2026-2-03', '2005-09-06') as result;

-- 流程控制函数
-- case when A then when B then else C end
select case
           when 1 = 1 then 'lcz'
           when 1 = 2 then 'hhy'
           else 'who' end as result;
-- 第二种写法
-- case A when B then C when D then E else F end
-- 解释：当A等于B时返回C，当A等于D时返回E，否则返回F
select case 100 when 50 then 'tom' when 100 then 'mary' else 'tim' end;
-- if(A, B, C)
-- 三目运算符：如果A为true，则返回B，否则返回C
select if(1 = 1, 'lcz', 'hhy') as result;
-- 案例
create table employee
(
    name     string,         --姓名
    sex      string,         --性别
    birthday string,         --出生年月
    hiredate string,         --入职日期
    job      string,         --岗位
    salary   double,         --薪资
    bonus    double,         --奖金
    friends  array<string>,  --朋友
    children map<string,int> --孩子
);

insert into employee
values ('张无忌', '男', '1980/02/12', '2022/08/09', '销售', 3000, 12000, array('阿朱', '小昭'), map('张小无', 8, '张小忌', 9)),
       ('赵敏', '女', '1982/05/18', '2022/09/10', '行政', 9000, 2000, array('阿三', '阿四'), map('赵小敏', 8)),
       ('宋青书', '男', '1981/03/15', '2022/04/09', '研发', 18000, 1000, array('王五', '赵六'), map('宋小青', 7, '宋小书', 5)),
       ('周芷若', '女', '1981/03/17', '2022/04/10', '研发', 18000, 1000, array('王五', '赵六'), map('宋小青', 7, '宋小书', 5)),
       ('郭靖', '男', '1985/03/11', '2022/07/19', '销售', 2000, 13000, array('南帝', '北丐'), map('郭芙', 5, '郭襄', 4)),
       ('黄蓉', '女', '1982/12/13', '2022/06/11', '行政', 12000, null, array('东邪', '西毒'), map('郭芙', 5, '郭襄', 4)),
       ('杨过', '男', '1988/01/30', '2022/08/13', '前台', 5000, null, array('郭靖', '黄蓉'), map('杨小过', 2)),
       ('小龙女', '女', '1985/02/12', '2022/09/24', '前台', 6000, null, array('张三', '李四'), map('杨小过', 2));

select month(replace(hiredate, '/', '-')) as month,
       count(*)                           as cn
from employee
group by month(replace(hiredate, '/', '-'));

--
select job,
       sum(if(sex = '男', 1, 0)) male,
       sum(if(sex = '女', 1, 0)) female
from employee
group by job
order by male;

-- 高级聚合函数
-- collect_list
select collect_list(job)
from employee;
-- collect_set 去重
select collect_set(job)
from employee;

-- 每个月入职的人数以及姓名
select month(replace(hiredate, '/', '-')) mouth, count(*) cnt, collect_list(name) names
from employee
group by month(replace(hiredate, '/', '-'));

select substring(stu_name, 1, 1) name,
       count(*)                  cnt
from student_info
group by substring(stu_name, 1, 1)
having cnt > 2;


-- 炸裂函数和窗口函数是hive的难点和重点！

-- UDTF函数: 用户自定义制表函数，英文：User-Defined Table-Generating Function
select explode(`array`(1, 2, 3));
--
select explode(`map`("a", 1, 2, 2, 'c', 3));
-- 注意是 , 不是 :

--
select posexplode(`array`(1, 2, 3));
-- 下标从0开始

-- inline函数
desc function extended inline;
-- 把结构体炸裂成多行
-- 结构体里面是 id:value


-- 炸裂函数案例
create table movie_info
(
    movie    string, --电影名称
    category string  --电影分类
)
    row format delimited fields terminated by "\t";

insert overwrite table movie_info
values ("《疑犯追踪》", "悬疑,动作,科幻,剧情"),
       ("《Lie to me》", "悬疑,警匪,动作,心理,剧情"),
       ("《战狼2》", "战争,动作,灾难");

-- 根据电影信息表，统计各分类的电影数量


select cate, count(*) cnt
from (
         select movie,
                split(category, ",") as category
         from movie_info
     ) t1 lateral view explode(category) tmp as cate -- 新的列字段
group by cate











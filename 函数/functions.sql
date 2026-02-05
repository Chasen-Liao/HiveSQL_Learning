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
                split(category, ",") as category -- split 分割为数组
         from movie_info
     ) t1 lateral view explode(category) tmp as cate -- 新的列字段
group by cate;



-- 窗口函数
-- lead & leg
select *,
       lead(salary, 1, 0) over (partition by job order by salary) as next_salary,
       lag(salary, 1, 0) over (partition by job order by salary) as pre_salary
from employee;
-- 不支持自定义窗口函数

-- first_value & last_value
-- 获取窗口的第一个值和最后一个值
-- 不用自己写窗口范围
select *,
       first_value(salary) over (partition by job order by salary) as first_salary,
       last_value(salary) over (partition by job order by salary) as last_salary
from employee;

-- 排名函数
-- rank & dense_rank & row_number
-- rank 函数在并列的时候会重复，1 1 3
-- dense_rank 函数在并列的时候 是密集的 dense是密集的意思，1 1 2
-- row_number 函数在并列的时候，会从1开始编号 1 2 3

select *,
       rank() over (partition by job order by salary desc) as rank,
       dense_rank() over (partition by job order by salary desc) as dense_rank,
       row_number() over (partition by job order by salary desc) as row_number
from employee;


-- 案例
create table order_info
(
    order_id     string, --订单id
    user_id      string, -- 用户id
    user_name    string, -- 用户姓名
    order_date   string, -- 下单日期
    order_amount int     -- 订单金额
);

insert overwrite table order_info
values ('1', '1001', '小元', '2022-01-01', '10'),
       ('2', '1002', '小海', '2022-01-02', '15'),
       ('3', '1001', '小元', '2022-02-03', '23'),
       ('4', '1002', '小海', '2022-01-04', '29'),
       ('5', '1001', '小元', '2022-01-05', '46'),
       ('6', '1001', '小元', '2022-04-06', '42'),
       ('7', '1002', '小海', '2022-01-07', '50'),
       ('8', '1001', '小元', '2022-01-08', '50'),
       ('9', '1003', '小辉', '2022-04-08', '62'),
       ('10', '1003', '小辉', '2022-04-09', '62'),
       ('11', '1004', '小猛', '2022-05-10', '12'),
       ('12', '1003', '小辉', '2022-04-11', '75'),
       ('13', '1004', '小猛', '2022-06-12', '80'),
       ('14', '1003', '小辉', '2022-04-13', '94');

-- 1. 统计每个用户截至每次下单的累积下单总额
select order_id,
       user_id,
       user_name,
       order_date,
       order_amount,
       sum(order_amount) over (partition by user_id order by order_date rows between unbounded preceding and current row) as sum_so_far
from order_info;

-- 2. 统计每个用户截至每次下单的当月累积下单总额
select order_id,
       user_id,
       user_name,
       order_date,
       order_amount,
       sum(order_amount) over (partition by substring(order_date, 1, 7) -- 要带上年份，要不然区分不了年份
           order by order_date rows between unbounded preceding and current row)
           as sum_so_far
from order_info;

-- 3. 统计每个用户每次下单距离上次下单相隔的天数（首次下单按0天算）
select order_id,
       user_id,
       user_name,
       order_date,
       order_amount, -- lag 取上面1行的值
       lag(order_date, 1, '2020-01-01') over (partition by user_id order by order_date) as order_amount,
       datediff(order_date, lag(order_date, 1, '2020-01-01') over (partition by user_id order by order_date)) as diff
from order_info;


-- 用子查询减少窗口函数的开销
select order_id,
       user_id,
       user_name,
       order_date,
       order_amount,
       nvl(datediff(order_date, pre_order_date), 0) as diff -- 如果是null也就说明是第一次下单，变为0
from
(
    select order_id,
           user_id,
           user_name,
           order_date,
           order_amount,
           lag(order_date, 1, null) over (partition by user_id order by order_date) as pre_order_date
    from order_info
) t1;

-- 3. 查询所有下单记录以及每个用户的每个下单记录所在月份的首/末次下单日期
select order_id,
       user_id,
       user_name,
       order_date,
       order_amount,
       first_value(order_date) over (partition by user_id, substring(order_date, 1, 7) order by order_date) as first_order_date,
       last_value(order_date) over (partition by user_id, substring(order_date, 1, 7) order by order_date
           rows between unbounded preceding and unbounded following) as last_order_date
from order_info; -- 错误！因为不显式指定窗口范围的话，
-- 有order by 默认帧是前面所有到当前行 range between unbounded preceding and current row
-- 不包含order by 的话，默认是rows between unbounded preceding and current row

-- 5. 为每个用户的所有下单记录按照订单金额进行排名
select order_id,
       user_id,
       user_name,
       order_date,
       order_amount,
       rank() over(partition by user_id order by order_amount desc) rk,
       dense_rank() over (partition by user_id order by order_amount desc) drk,
       row_number() over (partition by user_id order by order_amount desc) rn -- 里面不加参数！
from order_info;

-- 分组topN -- 找出前topN的数据，先拿子查询，子查询里面写窗口函数，然后where rk <= topN





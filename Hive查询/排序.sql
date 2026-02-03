--
use learn_hive;

-- order by 一般要加上limit，不然MR的Reduce会压力很大
-- 如果不加上limit的话，Reduce只有一个节点，MR的Reduce会压力很大
-- 而且一般只需要知道前几或者后几就可以了
select *
from emp
order by sal desc
limit 10;

-- desc 降序
-- asc 升序

-- sort by


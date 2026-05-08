-- 建表、导入数据
create table userbehavior (
    user_id bigint,
    item_id bigint,
    behavior enum('click','collect','cart','pay'),
    user_geohash varchar(10),
    category_id int,
    dt datetime
);
load data local infile 'D:/userbehavior.csv'
into table userbehavior
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows
(user_id, item_id, behavior, user_geohash, category_id, @ts)
set dt = str_to_date(
      concat(left(@ts, 10), ' ', lpad(substring_index(@ts, ' ', -1), 2, '0'), ':00:00'),
      '%Y-%m-%d %H:%i:%s'
);

-- 整体流量与转化指标
with user_pay as
(select count(distinct user_id) as uv,
	    count(case when behavior='click' then 1 end) as click_total,
	    count(case when behavior='pay' then 1 end) as pay_total
 from userbehavior)
select * from user_pay; #整体流量
with user_pay as
(select count(distinct user_id) as pay_cnt
 from userbehavior
 where behavior = 'pay'),
 user_click as
 (select count(distinct user_id) as click_cnt
  from userbehavior
  where behavior = 'click')
select pay_cnt / click_cnt as total_click_to_pay_ratio
from user_pay, user_click; #整体总支付转化率
with user_buy as
(select user_id, count(*) as buy_cnt
 from userbehavior
 where behavior = 'pay'
 group by user_id)
select count(case when buy_cnt>1 then 1 end) /
	   count(*) as repurchase_rate
from user_buy; #复购率

-- 全链路转化分析
with switch_stats as
(select count(case when behavior='click' then 1 end) as click_cnt,
		count(case when behavior='collect' then 1 end) as collect_cnt,
		count(case when behavior='cart' then 1 end) as cart_cnt,
		count(case when behavior='pay' then 1 end) as pay_cnt
 from userbehavior)
select collect_cnt / click_cnt as click_to_collect_rate,
	   cart_cnt / click_cnt as click_to_cart_rate,
	   pay_cnt / cart_cnt as cart_to_pay_rate,
	   pay_cnt / collect_cnt as collect_to_pay_rate
from switch_stats;

-- 时段流量与支付分布
select hour(dt) as h, 
	   count(distinct user_id) as uv
from userbehavior
group by h
order by uv desc;
select hour(dt) as h,
	   count(case when behavior='pay' then 1 end) as pay_cnt
from userbehavior
group by h
order by pay_cnt desc;

-- RFM用户分层分析
with user_pay as 
(select user_id, datediff('2014-12-18',date(dt)) as last_pay,
	    case when behavior='pay' then 1 end as behavior_pay
 from userbehavior
 where behavior = 'pay'),
rmf as (select user_id, count(*) as F,
                        min(last_pay) as R
        from user_pay
        group by user_id),
user_layers as (select case when R<7 and F>=3 then 'valuable'
					   when R<7 then 'potential'
					   when R>=7 and R <21 then 'asleep'
					   else 'lost' end as user_type
				from rmf)
select count(case when user_type='valuable' then 1 end) / count(*) as valuable,
	   count(case when user_type='potential' then 1 end) / count(*) as potential,
	   count(case when user_type='asleep' then 1 end) / count(*) as asleep,
	   count(case when user_type='lost' then 1 end) / count(*) as lost
from user_layers;

-- 类目销售分析
select category_id, count(*) as pay_cnt
from userbehavior
where behavior = 'pay'
group by category_id
order by pay_cnt desc
limit 10; #类目销售Top10
with top_categories as 
(select category_id 
 from userbehavior
 where behavior = 'pay'
 group by category_id
 order by count(*) desc
 limit 3),
category_switch as
(select category_id,
		count(case when behavior='collect' then 1 end) as collect_cnt,
		count(case when behavior='cart' then 1 end) as cart_cnt,
		count(case when behavior='pay' then 1 end) as pay_cnt
 from userbehavior
 where category_id in (select category_id from top_categories)
 group by category_id)
select category_id,
	   pay_cnt / cart_cnt as cart_to_pay_rate,
	   pay_cnt / collect_cnt as collect_to_pay_rate
from category_switch
order by pay_cnt desc; #Top3类目转化率
with category_pay as
(select category_id, count(*) as pay_cnt
 from userbehavior
 where behavior = 'pay'
 group by category_id),
total_stats as 
(select sum(pay_cnt) as total_pay
 from category_pay)
select category_id,
       pay_cnt / total_pay as pay_ratio
from category_pay, total_stats
order by pay_ratio desc; #类目支付订单比例

-- 商品关联购买
select a.item_id as item1, b.item_id as item2, count(*) as cnt
from userbehavior a join userbehavior b
on a.user_id = b.user_id and a.item_id < b.item_id
and a.behavior = 'pay' and b.behavior = 'pay'
group by item1, item2
having cnt >= 5
order by cnt desc
limit 10;
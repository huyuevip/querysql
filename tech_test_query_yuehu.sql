# original sql, need to be optimized and fully tested
with temp_volume_open_price_metrics as (
select percentile_disc(0.25) within group (order by trades.volume) as volume_q1,
percentile_disc(0.5) within group (order by trades.volume) as volume_q2,
percentile_disc(0.75) within group (order by trades.volume) as volume_q3,
(percentile_disc(0.75) within group (order by trades.volume)-percentile_disc(0.25) within group (order by trades.volume)) as volume_iqr,
percentile_disc(0.25) within group (order by trades.volume)-1.5* (percentile_disc(0.75) within group (order by trades.volume)-percentile_disc(0.25) within group (order by trades.volume)) as volume_lower_bound,
percentile_disc(0.75) within group (order by trades.volume)+1.5* (percentile_disc(0.75) within group (order by trades.volume)-percentile_disc(0.25) within group (order by trades.volume)) as volume_upper_bound,
percentile_disc(0.25) within group (order by trades.open_price) as open_price_q1,
percentile_disc(0.5) within group (order by trades.open_price) as open_price_q2,
percentile_disc(0.75) within group (order by trades.open_price) as open_price_q3,
(percentile_disc(0.75) within group (order by trades.open_price)-percentile_disc(0.25) within group (order by trades.open_price)) as open_price_iqr,
percentile_disc(0.25) within group (order by trades.open_price)-1.5* (percentile_disc(0.75) within group (order by trades.open_price)-percentile_disc(0.25) within group (order by trades.open_price)) as open_price_lower_bound,
percentile_disc(0.75) within group (order by trades.open_price)+1.5* (percentile_disc(0.75) within group (order by trades.open_price)-percentile_disc(0.25) within group (order by trades.open_price)) as open_price_upper_bound
from trades),
temp_users as (
select login_hash,server_hash,country_hash,currency,enable from users where enable=1 group by login_hash,server_hash,country_hash,currency,enable
),temp_base_original as(
select temp_volume_open_price_metrics.*,temp_users.*,trades.ticket_hash,trades.symbol,(case when (trades.volume >= temp_volume_open_price_metrics.volume_lower_bound and trades.volume <= temp_volume_open_price_metrics.volume_upper_bound) then trades.volume else temp_volume_open_price_metrics.volume_q2 end)as volume ,trades.open_time,trades.close_time from temp_volume_open_price_metrics cross join temp_users join trades on temp_users.login_hash=trades.login_hash and temp_users.server_hash=trades.server_hash 
),temp_cont_date as(
select to_date(to_char(a,'yyyy-MM-dd'),'yyyy-MM-dd') as dt_report from generate_series('2020-05-024'::date,'2020-09-30'::date, '1 days') as a
),temp_base_stage_1 as(
select temp_cont_date.dt_report,temp_base_original.login_hash,temp_base_original.server_hash,temp_base_original.symbol,temp_base_original.currency,temp_base_original.volume,temp_base_original.open_time,temp_base_original.close_time from temp_cont_date left join temp_base_original on temp_cont_date.dt_report=to_date(to_char(temp_base_original.open_time,'yyyy-MM-dd'),'yyyy-MM-dd')  order by dt_report
),temp_base as (
select temp_base_stage_1.dt_report,temp_base_original.login_hash,temp_base_original.server_hash,temp_base_original.symbol,temp_base_original.currency,temp_base_original.volume,temp_base_original.open_time,temp_base_original.close_time from temp_base_stage_1  left join temp_base_original on temp_base_stage_1.dt_report=to_date(to_char(temp_base_original.close_time,'yyyy-MM-dd'),'yyyy-MM-dd') where temp_base_stage_1.login_hash is null
union
select dt_report,login_hash,server_hash,symbol,currency,volume,open_time,close_time from temp_base_stage_1 where login_hash is not null
)select a.dt_report,a.login_hash,a.server_hash,a.symbol,a.currency,sum(volume) over (partition by a.login_hash,a.server_hash,a.symbol order by a.open_time range between interval '6' day preceding and current row) as sum_volume_prev_7d,b.sum_volume_prev_all,c.rank_volume_symbol_prev_7d,d.rank_count_prev_7d,f.sum_volume_2020_08,g.date_first_trade,h.row_number_ from temp_base a
join(
select dt_report,login_hash,server_hash,symbol,currency, sum(volume) over (partition by login_hash,server_hash,symbol order by open_time range between UNBOUNDED PRECEDING and current row) as sum_volume_prev_all from temp_base) b 
on a.dt_report=b.dt_report and a.login_hash=b.login_hash and a.server_hash=b.server_hash and a.symbol=b.symbol and a.currency=b.currency
join(
select dt_report,login_hash,server_hash,symbol,currency, dense_rank() over (partition by login_hash,symbol order by open_time range between interval '6' day preceding and current row) as rank_volume_symbol_prev_7d from temp_base) c
on a.dt_report=c.dt_report and a.login_hash=c.login_hash and a.server_hash=c.server_hash and a.symbol=c.symbol and a.currency=c.currency
join(
select dt_report,login_hash,server_hash,symbol,currency, dense_rank() over (partition by login_hash order by open_time range between interval '6' day preceding and current row) as rank_count_prev_7d from temp_base) d
on a.dt_report=d.dt_report and a.login_hash=d.login_hash and a.server_hash=d.server_hash and a.symbol=d.symbol and a.currency=d.currency
join(
select z.dt_report,z.login_hash,z.server_hash,z.symbol,z.currency, (case when z.open_time<='2020-8-31' and z.open_time>='2020-8-1' then z.sum_volume_prev_all else 0 end) as sum_volume_2020_08 from (
select dt_report,login_hash,server_hash,symbol,currency,open_time,sum(volume) over (partition by login_hash,server_hash,symbol order by open_time range between UNBOUNDED PRECEDING and current row) as sum_volume_prev_all from temp_base ) z ) f
on a.dt_report=f.dt_report and a.login_hash=f.login_hash and a.server_hash=f.server_hash and a.symbol=f.symbol and a.currency=f.currency
join(
select dt_report,login_hash,server_hash,symbol,currency,min(open_time) over (partition by login_hash,server_hash,symbol order by open_time range between UNBOUNDED PRECEDING and current row) as date_first_trade from temp_base) g
on a.dt_report=g.dt_report and a.login_hash=g.login_hash and a.server_hash=g.server_hash and a.symbol=g.symbol and a.currency=g.currency
join(
select dt_report,login_hash,server_hash,symbol,currency,row_number() over (order by dt_report,login_hash,server_hash,symbol) as row_number_ from temp_base) h
on a.dt_report=h.dt_report and a.login_hash=h.login_hash and a.server_hash=h.server_hash and a.symbol=h.symbol and a.currency=h.currency


# possible more efficient sql, need to be optimized and fully tested
with temp_volume_open_price_metrics as (
select percentile_disc(0.25) within group (order by trades.volume) as volume_q1,
percentile_disc(0.5) within group (order by trades.volume) as volume_q2,
percentile_disc(0.75) within group (order by trades.volume) as volume_q3,
(percentile_disc(0.75) within group (order by trades.volume)-percentile_disc(0.25) within group (order by trades.volume)) as volume_iqr,
percentile_disc(0.25) within group (order by trades.volume)-1.5* (percentile_disc(0.75) within group (order by trades.volume)-percentile_disc(0.25) within group (order by trades.volume)) as volume_lower_bound,
percentile_disc(0.75) within group (order by trades.volume)+1.5* (percentile_disc(0.75) within group (order by trades.volume)-percentile_disc(0.25) within group (order by trades.volume)) as volume_upper_bound,
percentile_disc(0.25) within group (order by trades.open_price) as open_price_q1,
percentile_disc(0.5) within group (order by trades.open_price) as open_price_q2,
percentile_disc(0.75) within group (order by trades.open_price) as open_price_q3,
(percentile_disc(0.75) within group (order by trades.open_price)-percentile_disc(0.25) within group (order by trades.open_price)) as open_price_iqr,
percentile_disc(0.25) within group (order by trades.open_price)-1.5* (percentile_disc(0.75) within group (order by trades.open_price)-percentile_disc(0.25) within group (order by trades.open_price)) as open_price_lower_bound,
percentile_disc(0.75) within group (order by trades.open_price)+1.5* (percentile_disc(0.75) within group (order by trades.open_price)-percentile_disc(0.25) within group (order by trades.open_price)) as open_price_upper_bound
from trades),
temp_users as (
select login_hash,server_hash,country_hash,currency,enable from users where enable=1 group by login_hash,server_hash,country_hash,currency,enable
),temp_base_original as(
select temp_volume_open_price_metrics.*,temp_users.*,trades.ticket_hash,trades.symbol,(case when (trades.volume >= temp_volume_open_price_metrics.volume_lower_bound and trades.volume <= temp_volume_open_price_metrics.volume_upper_bound) then trades.volume else temp_volume_open_price_metrics.volume_q2 end)as volume ,trades.open_time,trades.close_time from temp_volume_open_price_metrics cross join temp_users join trades on temp_users.login_hash=trades.login_hash and temp_users.server_hash=trades.server_hash 
),temp_cont_date as(
select to_date(to_char(a,'yyyy-MM-dd'),'yyyy-MM-dd') as dt_report from generate_series('2020-05-024'::date,'2020-09-30'::date, '1 days') as a
),temp_base_stage_1 as(
select temp_cont_date.dt_report,temp_base_original.login_hash,temp_base_original.server_hash,temp_base_original.symbol,temp_base_original.currency,temp_base_original.volume,temp_base_original.open_time,temp_base_original.close_time from temp_cont_date left join temp_base_original on temp_cont_date.dt_report=to_date(to_char(temp_base_original.open_time,'yyyy-MM-dd'),'yyyy-MM-dd')  order by dt_report
),temp_base as (
select temp_base_stage_1.dt_report,temp_base_original.login_hash,temp_base_original.server_hash,temp_base_original.symbol,temp_base_original.currency,temp_base_original.volume,temp_base_original.open_time,temp_base_original.close_time from temp_base_stage_1  left join temp_base_original on temp_base_stage_1.dt_report=to_date(to_char(temp_base_original.close_time,'yyyy-MM-dd'),'yyyy-MM-dd') where temp_base_stage_1.login_hash is null
union
select dt_report,login_hash,server_hash,symbol,currency,volume,open_time,close_time from temp_base_stage_1 where login_hash is not null
)select a.dt_report,a.login_hash,a.server_hash,a.symbol,a.currency,sum(volume) over (partition by a.login_hash,a.server_hash,a.symbol order by a.open_time range between interval '6' day preceding and current row) as sum_volume_prev_7d,sum(volume) over (partition by a.login_hash,a.server_hash,a.symbol order by open_time range between UNBOUNDED PRECEDING and current row) as sum_volume_prev_all,c.rank_volume_symbol_prev_7d,d.rank_count_prev_7d,f.sum_volume_2020_08,min(a.open_time) over (partition by a.login_hash,a.server_hash,a.symbol order by a.open_time range between UNBOUNDED PRECEDING and current row) as date_first_trade,h.row_number_ from temp_base a
join(
select dt_report,login_hash,server_hash,symbol,currency, dense_rank() over (partition by login_hash,symbol order by open_time range between interval '6' day preceding and current row) as rank_volume_symbol_prev_7d from temp_base) c
on a.dt_report=c.dt_report and a.login_hash=c.login_hash and a.server_hash=c.server_hash and a.symbol=c.symbol and a.currency=c.currency
join(
select dt_report,login_hash,server_hash,symbol,currency, dense_rank() over (partition by login_hash order by open_time range between interval '6' day preceding and current row) as rank_count_prev_7d from temp_base) d
on a.dt_report=d.dt_report and a.login_hash=d.login_hash and a.server_hash=d.server_hash and a.symbol=d.symbol and a.currency=d.currency
join(
select z.dt_report,z.login_hash,z.server_hash,z.symbol,z.currency, (case when z.open_time<='2020-8-31' and z.open_time>='2020-8-1' then z.sum_volume_prev_all else 0 end) as sum_volume_2020_08 from (
select dt_report,login_hash,server_hash,symbol,currency,open_time,sum(volume) over (partition by login_hash,server_hash,symbol order by open_time range between UNBOUNDED PRECEDING and current row) as sum_volume_prev_all from temp_base ) z ) f
on a.dt_report=f.dt_report and a.login_hash=f.login_hash and a.server_hash=f.server_hash and a.symbol=f.symbol and a.currency=f.currency
join(
select dt_report,login_hash,server_hash,symbol,currency,row_number() over (order by dt_report,login_hash,server_hash,symbol) as row_number_ from temp_base) h
on a.dt_report=h.dt_report and a.login_hash=h.login_hash and a.server_hash=h.server_hash and a.symbol=h.symbol and a.currency=h.currency

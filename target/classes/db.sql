select u.user_id,
        o.contact_phone,
        d.dep_city_name,
        d.arr_city_name
   from ( select order_id,passenger_uid,dep_city_name,arr_city_name  from s_airline_order_detail where dt >= '2017-07-01' ) d
   join ( select user_id,contact_phone,order_id,orderstatue from s_airline_order        where dt >= '2017-07-01' ) o
     on ( d.order_id = o.order_id and
        o.orderstatue IN ('5', '52', '53', '71', '73', '74', '75'))
   join s_user_info u
     on (u.user_id = o.user_id and
        (u.passport = d.passenger_uid OR u.id_card = d.passenger_uid))


select u.user_id,
        o.contact_phone,
        d.dep_city_name,
        d.arr_city_name
   from ( select order_id,passenger_uid,dep_city_name,arr_city_name  from s_airline_order_detail where dt >= '2017-07-01' ) d
   join ( select user_id,contact_phone,order_id,orderstatue from s_airline_order        where dt >= '2017-07-01' ) o
     on ( d.order_id = o.order_id and
        o.orderstatue IN ('5', '52', '53', '71', '73', '74', '75'))



select  o.user_id,
        o.contact_phone,
        d.dep_city_name,
        d.arr_city_name
   from s_airline_order_detail  d,
    s_airline_order o
    where o.dt >= '2017-07-01'   and o.dt=d.dt
     and d.order_id = o.order_id  and
        o.orderstatue IN ('5', '52', '53', '71', '73', '74', '75')



315 rows selected (0.03 seconds)
0: jdbc:hive2://master:50098> select count(1) from  weight_city_active;
+-----------+--+
|    _c0    |
+-----------+--+
| 10304627  |
+-----------+--+
1 row selected (14.272 seconds)
0: jdbc:hive2://master:50098> select count(1) from  weight_city_id_card;
+-----------+--+
|    _c0    |
+-----------+--+
| 50795313  |
+-----------+--+
1 row selected (12.685 seconds)
0: jdbc:hive2://master:50098> select count(1) from  weight_city_phone;
+---------+--+
|   _c0   |
+---------+--+
| 882346  |
+---------+--+
1 row selected (8.414 seconds)
0: jdbc:hive2://master:50098> select count(1) from  weight_city_travel;
+----------+--+
|   _c0    |
+----------+--+
| 2641812  |
+----------+--+
1 row selected (7.058 seconds)
0: jdbc:hive2://master:50098> select count(1) from  weight_city_phone_travel;
+----------+--+
|   _c0    |
+----------+--+
| 3329398  |
+----------+--+


select * from (
select count(1) city_active  from  weight_city_active ,
select count(1) city_id_card  from  weight_city_id_card,
select count(1) city_phone    from  weight_city_phone,
select count(1) city_travel   from  weight_city_travel,
select count(1) city_phone_travel from  weight_city_phone_travel ) temp



| ++BiAqX7ydfpRKugX208cQ==  | 上海       | 1   |
| ++BiAqX7ydfpRKugX208cQ==  | 昆明       | 2   |
| ++BiAqX7ydfpRKugX208cQ==  | 西安       | 3   |
| ++BiAqX7ydfpRKugX208cQ==  | 南京       | 4   |
| ++BiAqX7ydfpRKugX208cQ==  | 广州       | 5   |
| ++BiAqX7ydfpRKugX208cQ==  | 丽江       | 6   |
-------------------------------------------------

行程表
travel
| ++BiAqX7ydfpRKugX208cQ==  | 昆明    | 0.15  |
| ++BiAqX7ydfpRKugX208cQ==  | 上海    | 0.3   |
| ++BiAqX7ydfpRKugX208cQ==  | 西安    | 0.15  |


行程-联系电话表
0: jdbc:hive2://master:50098> select * from weight_city_phone_travel  where user_id='++BiAqX7ydfpRKugX208cQ=='
. . . . . . . . . . . . . . > ;
+---------------------------+----------------------+----------------+----------------+--+
|          user_id          |    contact_phone     | dep_city_name  | arr_city_name  |
+---------------------------+----------------------+----------------+----------------+--+
| ++BiAqX7ydfpRKugX208cQ==  | 1391750kUWisQWRkSs=  | 上海             | 西安             |
| ++BiAqX7ydfpRKugX208cQ==  | 1391750kUWisQWRkSs=  | 昆明             | 上海             |
+---------------------------+----------------------+----------------+----------------+--+

活跃城市
0: jdbc:hive2://master:50098> select * from  weight_city_active  where user_id='++BiAqX7ydfpRKugX208cQ=='
. . . . . . . . . . . . . . > ;
+---------------------------+-------+--------+--+
|          user_id          | city  |  rate  |
+---------------------------+-------+--------+--+
| ++BiAqX7ydfpRKugX208cQ==  | 丽江    | 0.025  |
| ++BiAqX7ydfpRKugX208cQ==  | 上海    | 0.125  |
| ++BiAqX7ydfpRKugX208cQ==  | 广州    | 0.025  |
| ++BiAqX7ydfpRKugX208cQ==  | 昆明    | 0.025  |
+---------------------------+-------+--------+--+

身份证地址
+---------------------------+-------+-------+--+
|          user_id          | city  | rate  |
+---------------------------+-------+-------+--+
| ++BiAqX7ydfpRKugX208cQ==  | 南京    | 0.1   |
+---------------------------+-------+-------+--+

手机归属地
0: jdbc:hive2://master:50098> select * from  weight_city_phone  where user_id='++BiAqX7ydfpRKugX208cQ=='
. . . . . . . . . . . . . . > ;
+---------------------------+-------+-------+--+
|          user_id          | city  | rate  |
+---------------------------+-------+-------+--+
| ++BiAqX7ydfpRKugX208cQ==  | 上海    | 0.1   |
+---------------------------+-------+-------+--+










use seckill;
-- 创建秒杀库存表
create table seckill
(
  `seckill_id`  bigint       NOT NULL AUTO_INCREMENT COMMENT '商品库存id',
  `name`        varchar(120) NOT NULL COMMENT '商品名称',
  `number`      int          NOT NULL COMMENT '库存数量',
  `start_time`  timestamp    NOT NULL COMMENT '秒杀开启时间',
  `end_time`    timestamp    NOT NULL COMMENT '秒杀结束时间',
  `create_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (seckill_id),
  KEY idx_start_time (start_time),
  KEY idx_end_time (end_time),
  KEY idx_create_time (create_time)
)ENGINE =InnoDB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8 COMMENT='秒杀库存表';
-- 初始化数据
insert into seckill(name,number,start_time,end_time)
values
       ('1000元秒杀Iphone6',100,'2019-11-01 00:00:00','2019-11-02 00:00:00'),
       ('500元秒杀小米9',200,'2019-11-01 00:00:00','2019-11-02 00:00:00'),
       ('600元秒杀华为',300,'2019-11-01 00:00:00','2019-11-02 00:00:00'),
       ('2000元秒杀笔记本',400,'2019-11-01 00:00:00','2019-11-02 00:00:00');

-- 秒杀成功明细表
-- 用户登录认证相关信息
create table success_killed(
  `seckill_id` bigint NOT NULL COMMENT '秒杀商品id',
  `user_phone`  bigint not null COMMENT '用户手机号',
  `state`  tinyint not null default -1 COMMENT '状态标志:-1:无效 0:成功 1:已付款 2:已发货',
  `create_time` timestamp not null DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (seckill_id, user_phone), /*联合主键*/
  KEY idx_create_time (create_time)
)ENGINE =InnoDB DEFAULT charset =utf8 comment='秒杀成功明细表'

-- 为什么要手写DDL
-- 记录每次上线的DDL修改



基于SpringMVC+Spring+Redis+MyBatis实现高并发秒杀API，分布式锁实现任务调度

## 关于权限管理
权限管理表，使用的是权限五表结构，如需做权限控制，修改
permission
->
role_permission
->
role
->
user_role
->
users

## 项目编译
升级了gradle编译，可以gradle编译也可以maven编译，但要注意羡慕在idea打开时选择对用文件，maven选择pom文件

## WEB前端页面逻辑
项目以高并发为主，故简化登录逻辑如下
详情页
   |
Login --no-- 登录页
   | yes       |
  展示      写入cookie


## 启动Springboot 
sudo nohup java -jar test.war



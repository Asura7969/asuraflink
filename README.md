# 环境安装

### mysql
mysql(8.0.31)

### 开启binlog
#### 1、新建mysql目录
* D:\docker-data\mysql\conf\conf.d
* D:\docker-data\mysql\conf\my.cnf
* D:\docker-data\mysql\data
* D:\docker-data\mysql\log
> conf.d 和 my.cnf 均为目录

**my.cnf**
```text
[client]
default-character-set=utf8
[mysql]
default-character-set=utf8
[mysqld]
log-bin=/var/lib/mysql/mysql-bin
server-id=123654
expire_logs_days = 30
default_authentication_plugin=mysql_native_password
```

**conf.d**
```text
[client]
default-character-set=utf8
[mysql]
default-character-set=utf8
[mysqld]
init_connect='SET collation_connection = utf8_unicode_ci'
init_connect='SET NAMES utf8'
character-set-server=utf8
collation-server=utf8_unicode_ci
skip-character-set-client-handshake
skip-name-resolve
```

### 运行
```shell
docker run --name mysql 
  -v D:/docker-data/mysql/log:/var/log/mysql 
  -v D:/docker-data/mysql/data:/var/lib/mysql 
  -v D:/docker-data/mysql/conf/conf.d:/etc/mysql/conf.d 
  -v D:/docker-data/mysql/conf/my.cnf:/etc/mysql/my.cnf 
  -e MYSQL_ROOT_PASSWORD=123456 
  -p 3306:3306 
  -d mysql:8.0.31
```

### 查看binlog
```shell
mysql -u root -p

show variables like '%log_bin%';

## 设置时区
SET GLOBAL time_zone = 'Asia/Shanghai';

show variables like '%time_zone%';
set time_zone = '+8:00';
set GLOBAL time_zone = '+8:00';
```

### 初始化
```shell
create database binlog;
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION;
ALTER USER 'root'@'%' IDENTIFIED WITH mysql_native_password BY '123456';
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '123456';
flush privileges;
select host,user,plugin,authentication_string from mysql.user;
```

### 建表
```sql
CREATE TABLE `t_order` (
   `order_id` varchar(100) NOT NULL DEFAULT '' COMMENT '订单id',
   `name` varchar(100) DEFAULT NULL COMMENT '用户名',
   `user_id` varchar(100) DEFAULT NULL COMMENT '用户id',
   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
   PRIMARY KEY (`order_id`),
   KEY `t_employee_job_number_IDX` (`user_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='订单表';
```

### kafka
kafka(2.5.1)

```shell
docker pull wurstmeister/kafka:2.12-2.5.1
```
`docker-compose.yml`
```yaml
version: '2'
services:
  zookeeper:
    image: wurstmeister/zookeeper
    ports:
      - "2181:2181"
  kafka:
    image: wurstmeister/kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_ADVERTISED_HOST_NAME: 127.0.0.1
      KAFKA_CREATE_TOPICS: "user_order_count:1:1"
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    volumes:
      - D:/docker-data/kafka/data:/var/run/docker.sock
```
> 检查 `D:/docker-data/kafka/data` 是否存在

#### 运行
```shell
docker-compose -f docker-compose-single-broker.yml up
```

#### 检查
```shell
kafka-topics.sh --zookeeper zookeeper:2181 --describe --topic user_order_count

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic user_order_count --from-beginning
```


## 端口问题
```shell
net stop winnat
net start winnat
```

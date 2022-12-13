# Docker 安装 mysql

## 安装mysql
通过docker desktop安装mysql

## 开启binlog
### 1、新建mysql目录
* D:\docker-data\mysql\conf
* D:\docker-data\mysql\data
* D:\docker-data\mysql\log

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

## 运行(管理员)
```shell
docker run --name mysql -v D:/docker-data/mysql/log:/var/log/mysql -v D:/docker-data/mysql/data:/var/lib/mysql -v D:/docker-data/mysql/conf:/etc/mysql/my.cnf -v D:/docker-data/mysql/conf:/etc/mysql/conf.d -e MYSQL_ROOT_PASSWORD=123456 -p 3306:3306 -d mysql:8.0.31 
```

## 查看binlog
```shell
mysql -u root -p

show variables like '%log_bin%';
```

## 初始化
```shell
create database binlog;
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION;
ALTER USER 'root'@'%' IDENTIFIED WITH mysql_native_password BY '123456';
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '123456';
flush privileges;
select host,user,plugin,authentication_string from mysql.user;
```

## 端口问题
```shell
net stop winnat
net start winnat
```

# Docker

## kafka && zookeeper

### 下载安装
> docker pull wurstmeister/zookeeper
> 
> docker pull wurstmeister/kafka

### 启动
```sh
## 启动server
docker run -d --name zookeeper -p 2181:2181 wurstmeister/zookeeper
### 运行完以上命令会显示容器ID

docker run -d --name kafka --publish 9092:9092 --link zookeeper --env KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 --env KAFKA_ADVERTISED_HOST_NAME=localhost --env KAFKA_ADVERTISED_PORT=9092 wurstmeister/kafka
### 运行完以上命令会显示容器ID

## 进入kafka server容器
docker exec -it [容器ID] /bin/bash
### docker exec -it 5de1ba2f741e53d5ba473551a1f65ed6bc258886e8e42904943dc769db4f3814 /bin/bash


## 创建topic
/opt/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic test

## 查看
/opt/kafka/bin/kafka-topics.sh --list --zookeeper zookeeper:2181

## 生产者
/opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

## 消费者
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning


## 删除topic
删除目录 /kafka/kafka-logs-5de1ba2f741e 下对应的topic
删除zk信息 zkCli 删除broker下面对应的topic
```
## java API 生产数据
```java


```



## 附录

### docker 其他命令
```sh
# 查看所有正在运行容器
docker ps
# containerId 是容器的ID
docker stop containerId

# 查看所有容器
docker ps -a
# 查看所有容器ID
docker ps -a -q

# stop停止所有容器
docker stop $(docker ps -a -q)
# remove删除所有容器
docker rm $(docker ps -a -q)

```



## 参考
> https://blog.csdn.net/lordwish/article/details/105800870
> https://www.jianshu.com/p/4cc49ba6c26c
### Flink On Docker

* 1、本地新建目录用于运行flink（可选）
* 2、新建**docker-compose.yaml**文件
```yaml
version: "3.7"
services:
  flink-jobmanager-01:
    image: flink:latest
    container_name: flink-jobmanager-01
    hostname: flink-jobmanager-01
    expose:
      - "6123"
    ports:
      - "8081:8081"
      - "5006:5006"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        parallelism.default: 2
        env.java.opts.jobmanager: "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006"

  flink-taskmanager-01:
    image: flink:latest
    container_name: flink-taskmanager-01
    hostname: flink-taskmanager-01
    expose:
      - "6121"
      - "6122"
    depends_on:
      - flink-jobmanager-01
    command: taskmanager
    links:
      - "flink-jobmanager-01:jobmanager"
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 3
        parallelism.default: 3
        env.java.opts.taskmanager: "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"

  flink-taskmanager-02:
    image: flink:latest
    container_name: flink-taskmanager-02
    hostname: flink-taskmanager-02
    expose:
      - "6121"
      - "6122"
    depends_on:
      - flink-jobmanager-01
    command: taskmanager
    links:
      - "flink-jobmanager-01:jobmanager"
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 3
        parallelism.default: 3
        env.java.opts.taskmanager: "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"

  flink-taskmanager-03:
    image: flink:latest
    container_name: flink-taskmanager-03
    hostname: flink-taskmanager-03
    expose:
      - "6121"
      - "6122"
    depends_on:
      - flink-jobmanager-01
    command: taskmanager
    links:
      - "flink-jobmanager-01:jobmanager"
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 3
        parallelism.default: 3
        env.java.opts.taskmanager: "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
```
* 3、运行: **docker-compose up -d**
* 4、访问: **http://127.0.0.1:8081**
* 5、结束: **docker-compose kill**


[flink官网](https://ci.apache.org/projects/flink/flink-docs-master/docs/deployment/resource-providers/standalone/docker/#advanced-customization)
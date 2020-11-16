package com.asura.hbase.observer;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * 添加协处理器
 * <pre>
 * 1、hdfs目录添加jar(协处理器)
 * 2、disable 'table1'
 * 3、alter 'table1','coprocessor'=>'hdfs:///hb-test.jar|com.asura.hbase.observer.MyHbaseObserver|1000'
 * 4、desc 'table1'
 * 5、enable 'table1'
 * 6、put,get,delete, ...
 * </pre>
 * 卸载协处理器
 * <pre>
 * 1、disable 'table1'
 * 2、alter 'table1',METHOD=>'table_att_unset',NAME=>'coprocessor$1'
 * 3、enable 'table1'
 * </pre>
 *
 * Observer:类似于RDBMS中的触发器,可以实现权限管理、优先级设置、监控、ddl 控制、二级索引等功能
 * 参考:https://blog.csdn.net/u012150370/article/details/86707595
 *     https://www.cnblogs.com/frankdeng/p/9310340.html
 * mvn clean assembly:assembly -Dmaven.test.skip=true
 */
public class MyHbaseObserver implements RegionObserver, RegionCoprocessor {

    private static final Logger logger = LoggerFactory.getLogger(MyHbaseObserver.class);
    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        logger.info("====Test Start====");
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        logger.info("====Test End====");
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) {
        logger.info("====Test prePut====");
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        logger.info("====Test postPut====");
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete, WALEdit edit, Durability durability) {
        logger.info("====Test preDelete====");
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        logger.info("====Test postDelete====");
    }
}
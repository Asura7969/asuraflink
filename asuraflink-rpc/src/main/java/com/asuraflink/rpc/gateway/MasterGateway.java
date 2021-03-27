package com.asuraflink.rpc.gateway;

import org.apache.flink.runtime.rpc.RpcGateway;

import java.util.Map;

/**
 * @author asura7969
 * @create 2021-03-27-15:18
 */
public interface MasterGateway extends RpcGateway {

    public void assignTask();

    public String registerWoker(String address);

    public Map<String, String> getAllRegisteredWorker();
}

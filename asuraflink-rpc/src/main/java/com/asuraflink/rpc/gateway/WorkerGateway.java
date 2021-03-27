package com.asuraflink.rpc.gateway;

import org.apache.flink.runtime.rpc.RpcGateway;

/**
 * @author asura7969
 * @create 2021-03-27-15:17
 */
public interface WorkerGateway extends RpcGateway {

    public String doWork();

    public void setUuid(String uuid);

    public String getUuid();

}

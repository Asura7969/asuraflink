package com.asuraflink.rpc.endpoint;

import com.asuraflink.rpc.gateway.WorkerGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;

/**
 * @author asura7969
 * @create 2021-03-27-15:22
 */
public class WokerEndpoint extends RpcEndpoint implements WorkerGateway {

    private String uuid;

    public WokerEndpoint(RpcService rpcService, String endpointId) {
        super(rpcService, endpointId);
    }

    public WokerEndpoint(RpcService rpcService) {
        super(rpcService);
    }

    @Override
    public String doWork() {
        System.out.println("woker 干活");
        return null;
    }

    @Override
    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    @Override
    public String getUuid() {
        return uuid;
    }
}

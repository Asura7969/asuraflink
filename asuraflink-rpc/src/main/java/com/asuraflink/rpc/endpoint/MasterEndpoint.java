package com.asuraflink.rpc.endpoint;

import com.asuraflink.rpc.gateway.MasterGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author asura7969
 * @create 2021-03-27-15:21
 */
public class MasterEndpoint extends RpcEndpoint implements MasterGateway {

    private Map<String, String> registeredWorker = new ConcurrentHashMap<>();

    public MasterEndpoint(RpcService rpcService, String endpointId) {
        super(rpcService, endpointId);
    }

    public MasterEndpoint(RpcService rpcService) {
        super(rpcService);
    }

    @Override
    public void assignTask() {

    }

    @Override
    public String registerWoker(String address) {
        String uuid = UUID.randomUUID().toString();
        registeredWorker.put(uuid, address);
        return uuid;
    }

    @Override
    public Map<String, String> getAllRegisteredWorker() {
        return registeredWorker;
    }
}

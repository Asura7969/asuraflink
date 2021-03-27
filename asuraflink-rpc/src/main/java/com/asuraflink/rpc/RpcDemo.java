package com.asuraflink.rpc;

import akka.actor.ActorSystem;
import akka.actor.Terminated;
import com.asuraflink.rpc.endpoint.MasterEndpoint;
import com.asuraflink.rpc.endpoint.WokerEndpoint;
import com.asuraflink.rpc.gateway.MasterGateway;
import com.asuraflink.rpc.gateway.WorkerGateway;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceConfiguration;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author asura7969
 * @create 2021-03-27-15:23
 *
 * RpcEndpoint:
 *      提供具体服务的实体的抽象
 * RpcGateway:
 *      用于远程调用的代理接口
 * RpcService:
 *      是 RpcEndpoint 的运行时环境
 * RpcServer:
 *      相当于 RpcEndpoint 自身的的代理对象
 *
 */
public class RpcDemo {
    private static final Time TIMEOUT = Time.seconds(10L);
    private static ActorSystem actorSystem = null;
    private static RpcService rpcService = null;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        start();

        MasterEndpoint master = new MasterEndpoint(rpcService);
        WokerEndpoint woker = new WokerEndpoint(rpcService);
        RpcService masterRpcService = master.getRpcService();
        RpcService wokerRpcService = woker.getRpcService();

        master.start();
        MasterGateway masterGateway = master.getSelfGateway(MasterGateway.class);

        masterGateway.assignTask();

        woker.start();
        WorkerGateway workerGateway = woker.getSelfGateway(WorkerGateway.class);
        assertFunc(master.getAllRegisteredWorker().size() == 0, "");


        // master 连接 worker
        CompletableFuture<WorkerGateway> connect = masterRpcService.connect(workerGateway.getAddress(), WorkerGateway.class);
        connect.handleAsync((gateway, throwable) -> {
            String uuid = master.registerWoker(gateway.getAddress());
            System.out.println("master 生成的uuid: " + uuid);
            gateway.setUuid(uuid);
            return "";
        }).join();

        Map<String, String> allRegisteredWorker = master.getAllRegisteredWorker();
        assertFunc(allRegisteredWorker.size() == 1, "worker没有注册成功");
        assertFunc(master.getAllRegisteredWorker().containsKey(workerGateway.getUuid()), "不包含uuid");


        close();
    }

    public static void assertFunc(boolean flag, String msg) {
        if(!flag) {
            throw new RuntimeException(msg);
        }
    }


    public static void start() {
        actorSystem = AkkaUtils.createDefaultActorSystem();
        // 创建 RpcService， 基于 AKKA 的实现
        rpcService = new AkkaRpcService(actorSystem, AkkaRpcServiceConfiguration.defaultConfiguration());
    }


    public static void close() {
        final CompletableFuture<Void> rpcTerminationFuture = rpcService.stopService();
        final CompletableFuture<Terminated> actorSystemTerminationFuture = FutureUtils.toJava(actorSystem.terminate());

        try {
            FutureUtils
                    .waitForAll(Arrays.asList(rpcTerminationFuture, actorSystemTerminationFuture))
                    .get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

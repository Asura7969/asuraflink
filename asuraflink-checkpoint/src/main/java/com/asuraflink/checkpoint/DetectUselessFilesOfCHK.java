package com.asuraflink.checkpoint;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
//import org.apache.flink.runtime.checkpoint.metadata.MetadataSerializer;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URL;
import java.sql.Savepoint;
import java.util.Map;

import static java.util.Collections.emptyList;

/**
 * https://mp.weixin.qq.com/s?__biz=MzkxOTE3MDU5MQ==&mid=2247484354&idx=1&sn=d9aa2180cb3eaec64515a8f2b018c1d4&source=41#wechat_redirect
 * 检测无用文件(checkpoint文件)
 */
public class DetectUselessFilesOfCHK {

//    static {
//        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
//    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration conf = new Configuration();
//        conf.set("fs.default.name", "hdfs://flashHadoopUAT");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        FileSystem fs = FileSystem.get(conf);

        Path remotePath = new Path("/flink/checkpoints/841574ada4bbb976dc254722f47e780b/chk-95703/_metadata");
        FileStatus[] status = fs.listStatus(remotePath);

        for (FileStatus file : status) {
            System.out.println(file.getPath());
        }

        FSDataInputStream in = fs.open(remotePath);

//        FileInputStream fis = new FileInputStream(f);
        BufferedInputStream bis = new BufferedInputStream(in);
        DataInputStream dis = new DataInputStream(bis);
        CheckpointMetadata metadata = new CheckpointMetadata(1L, emptyList(), emptyList(), null);

        CheckpointMetadata checkpointMetadata = Checkpoints.loadCheckpointMetadata(dis,
//                MetadataSerializer.class.getClassLoader());
                metadata.getClass().getClassLoader(), "");

        // 打印当前的 CheckpointId
        System.out.println(checkpointMetadata.getCheckpointId());

        // 遍历 OperatorState，这里的每个 OperatorState 对应一个 Flink 任务的 Operator 算子
        // 不要与 OperatorState  和 KeyedState 混淆，不是一个层级的概念
        for (OperatorState operatorState : checkpointMetadata.getOperatorStates()) {
            System.out.println(operatorState);
            // 当前算子的状态大小为 0 ，表示算子不带状态，直接退出
            if (operatorState.getStateSize() == 0) {
                continue;
            }

            // 遍历当前算子的所有 subtask
            for (OperatorSubtaskState operatorSubtaskState : operatorState.getStates()) {
                // 解析 operatorSubtaskState 的 ManagedKeyedState
                parseManagedKeyedState(operatorSubtaskState);
                // 解析 operatorSubtaskState 的 ManagedOperatorState
                parseManagedOperatorState(operatorSubtaskState);

            }
        }

    }

    /**
     * 解析 operatorSubtaskState 的 ManagedKeyedState
     *
     * @param operatorSubtaskState operatorSubtaskState
     */
    private static void parseManagedKeyedState(OperatorSubtaskState operatorSubtaskState) {
        // 遍历当前 subtask 的 KeyedState
        for (KeyedStateHandle keyedStateHandle : operatorSubtaskState.getManagedKeyedState()) {
            // 本案例针对 Flink RocksDB 的增量 Checkpoint 引发的问题，
            // 因此仅处理 IncrementalRemoteKeyedStateHandle
            if (keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
                // 获取 RocksDB 的 sharedState
                Map<StateHandleID, StreamStateHandle> sharedState =
                        ((IncrementalRemoteKeyedStateHandle) keyedStateHandle).getSharedState();
                // 遍历所有的 sst 文件，key 为 sst 文件名，value 为对应的 hdfs 文件 Handle
                for (Map.Entry<StateHandleID, StreamStateHandle> entry : sharedState.entrySet()) {
                    // 打印 sst 文件名
                    System.out.println("sstable 文件名：" + entry.getKey());
                    if (entry.getValue() instanceof FileStateHandle) {
                        org.apache.flink.core.fs.Path filePath = ((FileStateHandle) entry.getValue()).getFilePath();
                        // 打印 sst 文件对应的 hdfs 文件位置
                        System.out.println("sstable文件对应的hdfs位置:" + filePath.getPath());
                    }
                }
            }
        }
    }

    /**
     * 解析 operatorSubtaskState 的 ManagedOperatorState
     * 注：OperatorState 不支持 Flink 的 增量 Checkpoint，因此本案例可以不解析
     *
     * @param operatorSubState operatorSubtaskState
     */
    private static void parseManagedOperatorState(OperatorSubtaskState operatorSubState) {
        // 遍历当前 subtask 的 OperatorState
        for (OperatorStateHandle operatorStateHandle : operatorSubState.getManagedOperatorState().asList()) {
            StreamStateHandle delegateState = operatorStateHandle.getDelegateStateHandle();
            if (delegateState instanceof FileStateHandle) {
                org.apache.flink.core.fs.Path filePath = ((FileStateHandle) delegateState).getFilePath();
                System.out.println(filePath.getPath());
            }
        }
    }
}

package com.asuraflink.sink.batchinterval;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public abstract class BatchIntervalSinkOperator<T extends Serializable>
        extends AbstractStreamOperator<Object>
        implements ProcessingTimeCallback,OneInputStreamOperator<T,Object>{

    private List<T> list;
    private ListState<T> listState;
    private int batchSize;
    private long interval;
    private ProcessingTimeService processingTimeService;

    BatchIntervalSinkOperator(int batchSize, long interval){
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.batchSize = batchSize;
        this.interval = interval;
    }

    @Override
    public void open() throws Exception {
        super.open();
        if(interval > 0 && batchSize > 0){
            processingTimeService = getProcessingTimeService();
            long now = processingTimeService.getCurrentProcessingTime();
            processingTimeService.registerTimer(computeRegisterTime(interval,now),this);
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        this.list = new ArrayList<T>();
        listState = context.getOperatorStateStore().getSerializableListState("batch-interval-sink");
        if(context.isRestored()){
            listState.get().forEach(x -> list.add(x));
        }
    }


    @Override
    public void processElement(StreamRecord<T> element) {
        list.add(element.getValue());
        if (list.size() >= batchSize){
            saveRecords(list);
            list.clear();
        }
    }

    public abstract void saveRecords(List<T> batch);


    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        listState.clear();
        if (list.size() > 0){
            listState.addAll(list);
        }
    }

    @Override
    public void onProcessingTime(long timestamp) {
        if(interval > 0 && batchSize >= 1){
            long now = processingTimeService.getCurrentProcessingTime();
            getProcessingTimeService().registerTimer(computeRegisterTime(interval,now),this);
        }
        if(list.size() > 0){
            saveRecords(list);
            list.clear();
        }
    }

    /**
     * 计算最近一次注册时间 note:只支持 interval <= 60
     * @param interval 间隔时间
     * @param now  当前时间
     * @return
     */
    private static long computeRegisterTime(long interval,long now){

        long registerTime = now + interval;
        interval = interval / 1000;
        if(60 % interval == 0 && interval <= 60){
            now = now / 1000;
            int second = LocalDateTime.ofEpochSecond(now, 0, ZoneOffset.ofHours(8)).getSecond();
            registerTime = (now - second + (second / interval * interval + interval)) * 1000;
        }
        return registerTime;
    }
}

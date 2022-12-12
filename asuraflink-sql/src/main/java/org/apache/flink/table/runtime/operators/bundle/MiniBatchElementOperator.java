/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.bundle;

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;


public class MiniBatchElementOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, ProcessingTimeService.ProcessingTimeCallback {

    private static final long serialVersionUID = 1L;

    private final long intervalMs;
    private final long maxCount;

    private transient long currentWatermark;

    private transient int numOfElements = 0;

    private transient List<RowData> bundle;

    public MiniBatchElementOperator(long intervalMs, long maxCount) {
        this.intervalMs = intervalMs;
        this.maxCount = maxCount;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        super.open();

        currentWatermark = 0;
        this.bundle = new ArrayList<>();
        long now = getProcessingTimeService().getCurrentProcessingTime();
        getProcessingTimeService().registerTimer(now + intervalMs, this);

        // report marker metric
        getRuntimeContext()
                .getMetricGroup()
                .gauge("bundleSize", (Gauge<Integer>) () -> numOfElements);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        long now = getProcessingTimeService().getCurrentProcessingTime();
        long currentBatch = now - now % intervalMs;
        if (currentBatch > currentWatermark || numOfElements >= maxCount) {
            currentWatermark = currentBatch;
            // emit
            emit();
        } else {
            bundle.add(element.getValue());
            numOfElements ++;
        }
    }

    private GenericRowData toGenericRowData() {
        GenericRowData row = new GenericRowData(bundle.size());
        for (int i = 0; i < bundle.size(); ++i) {
            row.setField(i, bundle.get(i));
        }
        bundle.clear();
        numOfElements = 0;
        return row;
    }

    private void emit() {
        output.collect(new StreamRecord<>(toGenericRowData()));
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        long now = getProcessingTimeService().getCurrentProcessingTime();
        long currentBatch = now - now % intervalMs;
        if (currentBatch > currentWatermark) {
            currentWatermark = currentBatch;
            // emit
            emit();
//            output.emitWatermark(new Watermark(currentBatch));
        }
        getProcessingTimeService().registerTimer(currentBatch + intervalMs, this);
    }

    /**
     * Override the base implementation to completely ignore watermarks propagated from upstream (we
     * rely only on the {@link AssignerWithPeriodicWatermarks} to emit watermarks from here).
     */
    @Override
    public void processWatermark(Watermark mark) throws Exception {
        // if we receive a Long.MAX_VALUE watermark we forward it since it is used
        // to signal the end of input and to not block watermark progress downstream
        if (mark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
            currentWatermark = Long.MAX_VALUE;
//            output.emitWatermark(mark);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}

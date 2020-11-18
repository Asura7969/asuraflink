package com.asuraflink.project.driver;

import com.asuraflink.project.driver.pojo.DriverInfoData;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.io.File;
import java.net.URL;

public class DriverApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        URL fileUrl = DriverInfoData.class.getClassLoader().getResource("./driver/train.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
        PojoTypeInfo<DriverInfoData> pojoType =(PojoTypeInfo<DriverInfoData>) TypeExtractor.createTypeInfo(DriverInfoData.class);
        String[] field = new String[]{
                "id", "vendor_id", "pickup_datetime", "dropoff_datetime", "passenger_count",
                "pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude",
                "store_and_fwd_flag","trip_duration"};
        PojoCsvInputFormat<DriverInfoData> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, field);

        DataStreamSource<DriverInfoData> dataSource  = env.createInput(csvInput, pojoType);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        dataSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<DriverInfoData>(){
                    @Override
                    public long extractAscendingTimestamp(DriverInfoData data) {
                        return data.getPickup_datetime().getTime();
                    }
                }).keyBy("id");


        env.execute("DriverApp");

    }

}

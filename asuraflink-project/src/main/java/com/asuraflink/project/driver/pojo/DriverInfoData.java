package com.asuraflink.project.driver.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DriverInfoData {
    /* 行程id */
    private String id;
    /* 数据供应商id */
    private int vendor_id;
    /* 开始时间 */
    private Timestamp pickup_datetime;
    /* 结束时间 */
    private Timestamp dropoff_datetime;
    /* 乘客人数 */
    private int passenger_count;
    /* 上车经度 */
    private double pickup_longitude;
    /* 上车纬度 */
    private double pickup_latitude;
    /* 下车经度 */
    private double dropoff_longitude;
    /* 下车纬度 */
    private double dropoff_latitude;
    /* 行程记录是否存在车辆内存 Y/N */
    private String store_and_fwd_flag;
    /* 行程持续时间 s */
    private int trip_duration;
}
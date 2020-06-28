package com.demo.analysis.log;

import com.alibaba.fastjson.JSON;
import com.demo.base.AbstractSparkSql;
import com.demo.bean.log.NearStationLog;
import com.demo.util.DateUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;
import java.io.Serializable;

public class TrainNearStationLog extends AbstractSparkSql implements Serializable {

    public static final String TRAIN_NEAR_STATION_LOG = "viewfs:///data/twms/traffichuixing/wukong_near_station_product";

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        String date = DateUtils.produceDateOrYesterday(args);
        Dataset<String> originData = onlineTable(spark, date);
        Dataset<NearStationLog> nearStationLogData = originData.map((MapFunction<String, NearStationLog>) (s -> {
            try {
                NearStationLog nearStationLog = JSON.parseObject(s, NearStationLog.class);
                return nearStationLog;
            } catch (Exception e) {
                logger.error("somethins wrong with:" + s + "," + e.getMessage(), e);
            }
            return new NearStationLog();
        }), Encoders.bean(NearStationLog.class)).where("requestTime is not null").repartition(1);

        nearStationLogData.createOrReplaceTempView("nearStationLog");
        spark.sql("insert overwrite table tmp_trafficwisdom.train_near_station_log PARTITION(log_date ='" + date + "' ) select " +
                "requestId,originFrom,originTo,date,fromDataType,toDataType,sceneCode,stationCode,stationName,nearStationCode,nearStationName,distance,transportNum," +
                "lastArriveTime,lastLeaveTime,typeCode,from_unixtime(unix_timestamp(requestTime, 'yyyyMMddHHmmss')) as request_time from nearStationLog");
    }

    private Dataset<String> offlineTable(SparkSession spark, String date) {
        Dataset<String> tableData = spark.read().textFile("D:\\near_station_log.txt");
        return tableData;
    }

    private Dataset<String> onlineTable(SparkSession spark, String date) {
        Dataset<String> tableData = spark.read().textFile(TRAIN_NEAR_STATION_LOG + "/" + date + "/*").distinct();
        return tableData;
    }

    public static void main(String[] args) throws IOException {
        TrainNearStationLog nearStationLog = new TrainNearStationLog();
        nearStationLog.runAll(args, true);
    }
}

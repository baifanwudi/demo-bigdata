package com.demo.analysis.spider;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.demo.base.AbstractReadDiffEnvTable;
import com.demo.bean.spider.TrainStopoverSplitBO;
import com.demo.util.DateUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class TrainStopoverSplit extends AbstractReadDiffEnvTable implements Serializable {

    private static final DateTimeFormatter TIME_HOUR_MIN_FORMAT=DateTimeFormatter.ofPattern("HH:mm");

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        Dataset<Row> tableData = readTable(spark,args);

        Dataset<TrainStopoverSplitBO> trainStopOverData = tableData.flatMap((FlatMapFunction<Row, TrainStopoverSplitBO>) row -> {
            List<TrainStopoverSplitBO> trainStopoverSplitBOList = new ArrayList<TrainStopoverSplitBO>();
            try {
                String trainNo = row.<String>getAs("train_no").trim();
                String transportCode = row.<String>getAs("train_code").trim();
                String durationDate = row.<String>getAs("date").trim();
                String data = row.<String>getAs("data").trim();
                JSONArray array = JSON.parseArray(data);
                int size = array.size();
                if (size == 0) {
                    return trainStopoverSplitBOList.iterator();
                }
                int day = 0;
                String prefixTime = null;
                for (int i = 0; i < size && size > 0; i++) {
                    TrainStopoverSplitBO trainStopoverSplitBO = new TrainStopoverSplitBO();
                    trainStopoverSplitBO.setTrainNo(trainNo);
                    trainStopoverSplitBO.setTransportCode(transportCode);
                    trainStopoverSplitBO.setDurationDate(durationDate);
                    JSONObject object = array.getJSONObject(i);
                    String stationName = object.getString("station_name").trim();
                    String arriveTime = object.getString("arrive_time").trim();
                    String leaveTime = object.getString("start_time").trim();
                    if (arriveTime.contains("-")) {
                        arriveTime = leaveTime;
                    }
//                    Integer stationSequence = object.getInteger("station_no");
                    //重排序
                    Integer stationSequence=i+1;
                    trainStopoverSplitBO.setStationName(stationName);
                    trainStopoverSplitBO.setArriveTime(arriveTime);
                    trainStopoverSplitBO.setLeaveTime(leaveTime);
                    trainStopoverSplitBO.setStationSequence(stationSequence);
                    String sleepTime = object.getString("stopover_time").trim();
                    if (sleepTime.indexOf("-") == -1) {
                        trainStopoverSplitBO.setSleepTime(DateUtils.calculateStrTimeToMin(sleepTime));
                    } else {
                        trainStopoverSplitBO.setSleepTime(0);
                    }
                    if (prefixTime == null) {
                    } else if (LocalTime.parse(arriveTime,TIME_HOUR_MIN_FORMAT).compareTo(LocalTime.parse(prefixTime,TIME_HOUR_MIN_FORMAT)) < 0) {
                        day += 1;
                    } else if (LocalTime.parse(leaveTime,TIME_HOUR_MIN_FORMAT).compareTo(LocalTime.parse(arriveTime,TIME_HOUR_MIN_FORMAT)) < 0) {
                        day += 1;
                    }
                    prefixTime = leaveTime;
                    trainStopoverSplitBO.setAfterDay(day);
                    trainStopoverSplitBOList.add(trainStopoverSplitBO);
                }
            } catch (Exception e) {
                logger.error("row is :" + row + " ; the error reason is:" + e.getMessage());
            }
            return trainStopoverSplitBOList.iterator();
        }, Encoders.bean(TrainStopoverSplitBO.class)).distinct().repartition(1);

        trainStopOverData.createOrReplaceTempView("trainStopOverData");
        spark.sql("insert overwrite table mid_trafficwisdom.train_stopover_split select A.trainNo,A.transportCode,B.station_code,A.stationName,A.stationSequence,A.durationDate,A.arriveTime,A.leaveTime,A.sleepTime,A.afterDay," +
                "current_date() as create_date from trainStopOverData A left join (SELECT * FROM base_tcdctrafficwisdomwukongbase.city_station_map WHERE traffic_type = 'T') B  ON A.stationName = B.station_name ");
    }

    @Override
    protected Dataset<Row> offlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.read()
                .json("D:\\data\\json\\crawl_train_stopover_data.json")
                .limit(2);
        tableData.show(false);
        tableData.printSchema();
        return tableData;
    }

    @Override
    protected Dataset<Row> onlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.sql("SELECT * FROM mid_trafficwisdom.crawl_train_stopover_data ");
        return tableData;
    }

    public static void main(String[] args) throws IOException {
        TrainStopoverSplit trainStopoverSplit = new TrainStopoverSplit();
        trainStopoverSplit.runAll(args, true);
    }
}

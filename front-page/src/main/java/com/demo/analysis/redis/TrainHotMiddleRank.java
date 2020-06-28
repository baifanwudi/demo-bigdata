package com.demo.analysis.redis;

import com.alibaba.fastjson.JSONObject;
import com.demo.base.AbstractReadDiffEnvTable;
import com.demo.bean.redis.TrainStopOverBO;
import com.demo.conf.RedisConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.collect_set;

/**
 * 经停线路中热门站点
 */
public class TrainHotMiddleRank extends AbstractReadDiffEnvTable {

    public static void main(String[] args) throws IOException {
        TrainHotMiddleRank hotStation = new TrainHotMiddleRank();
        hotStation.runAll(args, true);
    }

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        Dataset<Row> tableData = readTable(spark,args);
        Dataset<Row> middleStationSet = tableData.groupBy("transport_code", "start_station_code", "end_station_code")
                .agg(collect_set(functions.struct("middle_station_code","middle_station_name", "rank_num", "diff_sequence","diff_run_time")).as("middle_station_set"));
        saveRedis(middleStationSet);
    }

    private void saveRedis(Dataset<Row> dataSet) {
        dataSet.foreachPartition(
                data -> {
                    Jedis jedis = new Jedis("localhost");
                    SetParams setParams=new SetParams().ex(RedisConfig.REDIS_KEY_ALIVE);
                    while (data.hasNext()) {
                        Row record = data.next();
                        List<TrainStopOverBO> trainStopOverBOList = new ArrayList<>();
                        String key = String.format("trainStopOver:%s@%s@%s", record.<String>getAs("transport_code"), record.<String>getAs("start_station_code"), record.<String>getAs("end_station_code"));
                        WrappedArray<Row> combinedList = record.<WrappedArray<Row>>getAs("middle_station_set");
                        List<Row> rowList = new ArrayList<Row>(JavaConversions.<Row>seqAsJavaList(combinedList));
                        for (Row row : rowList) {
                            TrainStopOverBO trainStopOverBO = new TrainStopOverBO();
                            String midStationCode = row.<String>getAs("middle_station_code");
                            String midStationName = row.<String>getAs("middle_station_name");
                            Integer rankNum = row.<Integer>getAs("rank_num");
                            Integer diffSequence = row.<Integer>getAs("diff_sequence");
                            Integer diffRunTime=row.<Integer>getAs("diff_run_time");
                            trainStopOverBO.setStationName(midStationName);
                            trainStopOverBO.setStationCode(midStationCode);
                            trainStopOverBO.setRank(rankNum);
                            trainStopOverBO.setDiffSequence(diffSequence);
                            trainStopOverBO.setDiffRunTime(diffRunTime);
                            trainStopOverBOList.add(trainStopOverBO);
                        }
                        String value = JSONObject.toJSONString(trainStopOverBOList);
                        if (isLocalEnvConfig()) {
                            System.out.println("key:" + key + ",value:" + value);
                        }
                        jedis.set(key,  value, setParams);
                    }
                }
        );
    }

    @Override
    protected Dataset<Row> onlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.sql("select * from mid_trafficwisdom.train_stopover_middle_rank");
        return tableData;
    }

    @Override
    protected Dataset<Row> offlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.read()
                .option("inferschema", "true")
                .option("header", "true")
                .option("encoding", "gbk")
                .csv("D:\\data\\csv\\train_stopover_middle_rank.csv");
        tableData.show(false);
        tableData.printSchema();
        return tableData;
    }
}

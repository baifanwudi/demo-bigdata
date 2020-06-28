package com.demo.analysis.redis;

import com.alibaba.fastjson.JSONObject;
import com.demo.base.AbstractReadDiffEnvTable;
import com.demo.bean.redis.PlanEncryptBO;
import com.demo.conf.RedisConfig;
import com.demo.util.DesUtils;
import com.demo.util.DateUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import static org.apache.spark.sql.functions.*;

/**
 * 查询该用户是否有点击至填写页的方案（需要车次号、中转线路、始发城市完全一致），如果有且有多个，则显示最近的一个并标签
 * 返回标签：最近浏览
 */
public class StatsUserLatestClick extends AbstractReadDiffEnvTable {

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {

        Dataset<Row> tableData = readTable(spark, args).withColumn("create_time_long", unix_timestamp(col("create_time"), "yyyy-MM-dd HH:mm:ss")).drop("create_time").withColumnRenamed("create_time_long", "create_time");
        WindowSpec w = Window.partitionBy("member_id", "union_id", "first_traffic_type", "start_city_id", "start_station_code", "second_traffic_type", "end_city_id", "second_traffic_type", "end_station_code").orderBy(col("create_time").asc_nulls_last());
        Dataset<Row> userLatestClick = tableData.withColumn("rank", row_number().over(w)).where(col("rank").equalTo(1)).drop("rank");
        saveRedis(userLatestClick);
    }

    @Override
    protected Dataset<Row> offlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.read()
                .option("inferschema", "true")
                .option("header", "true")
                .option("encoding", "gbk")
                .csv("/Users/allenbai/Downloads/data/csv/transfer_action_log.csv").filter(col("first_traffic_type").equalTo("B"));
        tableData.printSchema();
        tableData.show(false);
        return tableData;
    }

    @Override
    protected Dataset<Row> onlineTable(SparkSession spark, String[] args) {
        Integer subDay = args.length < 1 ? -7 : Integer.parseInt(args[0]);
        String date = DateUtils.nowAfterOrBeforeDay(subDay);
        Dataset<Row> tableData = spark.sql("SELECT * FROM app_trafficwisdom.transfer_action_log " +
                "where log_date>='" + date + "' and user_action='click' and start_city_id!='' and transfer_city_id!='' and end_city_id!=''  ");
        return tableData;
    }

    private void saveRedis(Dataset<Row> dataSet) {
        Dataset<Row> combineData = dataSet.withColumn("combined", functions.struct("member_id", "union_id", "first_traffic_type", "first_traffic_code", "start_city_id", "start_city_name",
                "start_station_code", "start_station_name", "transfer_arrive_station_code", "transfer_arrive_station_name", "transfer_city_id", "transfer_city_name",
                "second_traffic_type", "second_traffic_code", "transfer_leave_station_code", "transfer_leave_station_name",
                "end_city_id", "end_city_name", "end_station_code", "end_station_name", "create_time"))
                .groupBy("start_city_id", "end_city_id", "member_id", "union_id").agg(collect_list("combined").as("combined_list")).repartition(4);
        combineData.foreachPartition(
                data -> {
                    Jedis jedis = new Jedis("localhost");
                    SetParams setParams=new SetParams().ex(RedisConfig.REDIS_KEY_ALIVE);
                     while (data.hasNext()) {
                        Row record = data.next();
                        List<PlanEncryptBO> md5EncryptList = new ArrayList<PlanEncryptBO>();
                        String key = String.format("recentLookPlan:%s@%s@%s@%s", record.<String>getAs("start_city_id"), record.<String>getAs("end_city_id"), record.<String>getAs("member_id"), record.<String>getAs("union_id"));
                        WrappedArray<Row> combinedList = record.<WrappedArray<Row>>getAs("combined_list");
                        List<Row> rowList = new ArrayList<Row>(JavaConversions.<Row>seqAsJavaList(combinedList));
                        for (Row row : rowList) {
                            String startCityName = row.<String>getAs("start_city_name");
                            String endCityName = row.<String>getAs("end_city_name");
                            String transferCityName = row.<String>getAs("transfer_city_name");
                            String firstTrafficType = row.<String>getAs("first_traffic_type");
                            String firstTrafficCode = row.<String>getAs("first_traffic_code");
                            String startStationCode = row.<String>getAs("start_station_code");
                            String transferArriveStationCode = row.<String>getAs("transfer_arrive_station_code");
                            String secondTrafficType = row.<String>getAs("second_traffic_type");
                            String secondTrafficCode = row.<String>getAs("second_traffic_code");
                            String transferLeaveStationCode = row.<String>getAs("transfer_leave_station_code");
                            String endStationCode = row.<String>getAs("end_station_code");
                            Long createTime = row.<Long>getAs("create_time");
                            String md5 = DesUtils.encoderByMd5(new StringBuffer().append(startCityName).append(endCityName).append(transferCityName).append(startStationCode).append(transferArriveStationCode).append(firstTrafficType).append(transferLeaveStationCode).append(endStationCode).append(secondTrafficType).append(firstTrafficCode).append(secondTrafficCode).toString());
                            PlanEncryptBO planEncryptBO = new PlanEncryptBO();
                            planEncryptBO.setPlanKey(md5);
                            planEncryptBO.setTransferCity(transferCityName);
                            planEncryptBO.setTransferType(firstTrafficType + secondTrafficType);
                            planEncryptBO.setCreateTime(createTime);
                            md5EncryptList.add(planEncryptBO);
                        }
                        String value = JSONObject.toJSONString(md5EncryptList);
                        if (isLocalEnvConfig()) {
                            System.out.println("key:" + key + ",value:" + value);
                        }
                         jedis.set(key,  value, setParams);
                    }
                }
        );
    }

    public static void main(String[] args) throws IOException {
        StatsUserLatestClick hotCreateRoute = new StatsUserLatestClick();
        hotCreateRoute.runAll(args, true);
    }
}

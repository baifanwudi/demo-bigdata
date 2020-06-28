package com.demo.analysis.redis;

import com.alibaba.fastjson.JSONObject;
import com.demo.base.AbstractReadDiffEnvTable;
import com.demo.bean.redis.PlanEncryptBO;
import com.demo.conf.RedisConfig;
import com.demo.util.DateUtils;
import com.demo.util.DesUtils;
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
import static org.apache.spark.sql.functions.*;

/**
 * 热门方案：与该查询会员无关，返回该出发到达地的创单数量最多的1条方案（需要车次号、中转地、有余票的最低价格完全一致）
 * 返回标签：热门方案
 */
public class StatsHotRouteOrder extends AbstractReadDiffEnvTable {

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        Dataset<Row> tableData = readTable(spark, args);
        Dataset<Row> routeOrderNum = tableData.groupBy("first_traffic_type", "first_traffic_code", "start_city_id", "start_city_name", "start_station_code", "start_station_name",
                "transfer_city_id", "transfer_city_name", "transfer_arrive_station_code", "transfer_arrive_station_name",
                "second_traffic_type", "second_traffic_code", "transfer_leave_station_code", "transfer_leave_station_name",
                "end_city_id", "end_city_name", "end_station_code", "end_station_name").agg(sum("ticket_num").as("total_num"))
                .filter(col("total_num").gt(10)).repartition(4);
        saveRedis(routeOrderNum);
    }

    @Override
    protected Dataset<Row> offlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.read()
                .option("inferschema", "true")
                .option("header", "true")
                .option("encoding", "gbk")
                .csv("D:\\data\\csv\\wisdom_traffic_order_route.csv");
        return tableData;
    }

    @Override
    protected Dataset<Row> onlineTable(SparkSession spark, String[] args) {
        Integer subDay = args.length < 1 ? -30 : Integer.parseInt(args[0]);
        String date = DateUtils.nowAfterOrBeforeDay(subDay);
        String ttSql = "SELECT first_traffic_type, first_traffic_code, start_city_id," +
                " start_city_name,start_station_code, start_station_name, transfer_arrive_station_code, transfer_arrive_station_name," +
                " transfer_city_id, transfer_city_name,second_traffic_type, second_traffic_code, transfer_leave_station_code, " +
                "transfer_leave_station_name,end_city_id, end_city_name, end_station_code,end_station_name,ticket_num " +
                "FROM mid_trafficwisdom.wisdom_traffic_order_route where create_time>='" + date + " 00:00:00'";
        Dataset<Row> tableData = spark.sql(ttSql);
        return tableData;
    }

    private void saveRedis(Dataset<Row> dataSet) {
        Dataset<Row> combineData = dataSet.withColumn("combined", functions.struct("first_traffic_type", "first_traffic_code", "start_city_id", "start_city_name",
                "start_station_code", "start_station_name", "transfer_arrive_station_code", "transfer_arrive_station_name", "transfer_city_id", "transfer_city_name",
                "second_traffic_type", "second_traffic_code", "transfer_leave_station_code", "transfer_leave_station_name",
                "end_city_id", "end_city_name", "end_station_code", "end_station_name", "total_num"))
                .groupBy("start_city_id", "end_city_id").agg(collect_list("combined").as("combined_list")).repartition(4);

        combineData.foreachPartition(
                data -> {
                    Jedis jedis = new Jedis("localhost");
                    SetParams setParams=new SetParams().ex(RedisConfig.REDIS_KEY_ALIVE);
                    while (data.hasNext()) {
                        Row record = data.next();
                        List<PlanEncryptBO> md5EncryptList = new ArrayList<PlanEncryptBO>();
                        String key = String.format("hotPlan:%s@%s", record.<String>getAs("start_city_id"), record.<String>getAs("end_city_id"));
                        WrappedArray<Row> combinedList = record.<WrappedArray<Row>>getAs("combined_list");
                        List<Row> rowList = new ArrayList<Row>(JavaConversions.<Row>seqAsJavaList(combinedList));
                        for (Row row : rowList) {
                            String startCityName = row.<String>getAs("start_city_name");
                            String endCityName = row.<String>getAs("end_city_name");
                            String transferCityName = row.<String>getAs("transfer_city_name");
                            String startStationCode = row.<String>getAs("start_station_code");
                            String transferArriveStationCode = row.<String>getAs("transfer_arrive_station_code");
                            String firstTrafficType = row.<String>getAs("first_traffic_type");
                            String transferLeaveStationCode = row.<String>getAs("transfer_leave_station_code");
                            String endStationCode = row.<String>getAs("end_station_code");
                            String secondTrafficType = row.<String>getAs("second_traffic_type");
                            String firstTrafficCode = row.<String>getAs("first_traffic_code");
                            String secondTrafficCode = row.<String>getAs("second_traffic_code");
                            Long totalNum = row.<Long>getAs("total_num");
                            String md5 = DesUtils.encoderByMd5(new StringBuffer().append(startCityName).append(endCityName).append(transferCityName).append(startStationCode).append(transferArriveStationCode).append(firstTrafficType).append(transferLeaveStationCode).append(endStationCode).append(secondTrafficType).append(firstTrafficCode).append(secondTrafficCode).toString());
                            PlanEncryptBO planEncryptBO = new PlanEncryptBO();
                            planEncryptBO.setTotalNum(totalNum);
                            planEncryptBO.setPlanKey(md5);
                            planEncryptBO.setTransferCity(transferCityName);
                            planEncryptBO.setTransferType(firstTrafficType + secondTrafficType);
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
        StatsHotRouteOrder hotRouteOrder = new StatsHotRouteOrder();
        hotRouteOrder.runAll(args, true);
    }
}

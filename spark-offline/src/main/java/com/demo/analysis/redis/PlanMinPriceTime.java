package com.demo.analysis.redis;

import com.alibaba.fastjson.JSONObject;
import com.demo.base.AbstractReadDiffEnvTable;
import com.demo.bean.redis.PlanMinPriceTimeBO;
import com.demo.common.ColumnUtil;
import com.demo.conf.RedisConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;
import scala.collection.Seq;

import java.io.IOException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;

public class PlanMinPriceTime extends AbstractReadDiffEnvTable {

    public final static Long REDIS_KEY_ALIVE=3600*24*4L;//4天

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        Dataset<Row> minPricePlan=getMinPricePlan(spark,args);
        minPricePlan.cache();
        Dataset<Row> minTimePlan=getMinTimePlan(spark,args);
        minTimePlan.cache();
        Seq<String> seq = ColumnUtil.columnNames("start_city_id,start_city_name,end_city_id,end_city_name,first_traffic_type,second_traffic_type");
        Dataset<Row> fullTable=minPricePlan.join(minTimePlan,seq,"outer").repartition(4);
        saveRedis(fullTable);
        minPricePlan.unpersist();
        minTimePlan.unpersist();
    }

    @Override
    protected Dataset<Row> offlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.read()
                .option("inferschema", "true")
                .option("header", "true")
                .option("encoding", "gbk")
                .csv("D:\\data\\csv\\train_flight_min_price.csv").filter(col("first_price").gt(0)).filter(col("second_price").gt(0))
                ;
        tableData.printSchema();
        tableData.show(false);
        return tableData;
    }

    @Override
    protected Dataset<Row> onlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.sql("SELECT * FROM app_trafficwisdom.route_traffic_transfer_without_date where first_traffic_type='T' " +
                "and second_traffic_type='T' and first_price>0 and second_price>0 ");
        return tableData;
    }

    private Dataset<Row> getMinPricePlan(SparkSession spark,String[] args){
        Dataset<Row> tableData = readTable(spark, args).withColumn("total_price", col("first_price").plus(col("second_price")));
        WindowSpec w = Window.partitionBy("start_city_id", "end_city_id","first_traffic_type","second_traffic_type").orderBy(col("total_price").asc_nulls_last());
        Dataset<Row> minPricePlan = tableData.withColumn("price_rank", row_number().over(w)).where(col("price_rank").equalTo(1)).drop("price_rank")
                .selectExpr("start_city_id","start_city_name","transfer_city_id as transfer_min_price_id","transfer_city_name as transfer_min_price_name","end_city_id","end_city_name","first_traffic_type","second_traffic_type","total_price");
        return minPricePlan;
    }

    private Dataset<Row> getMinTimePlan(SparkSession spark,String[] args){
        Dataset<Row> tableData = readTable(spark,args).withColumn("total_time", col("first_run_time").plus(col("second_run_time")).plus(col("sleep_time")));
        WindowSpec w = Window.partitionBy("start_city_id", "end_city_id","first_traffic_type","second_traffic_type").orderBy(col("total_time").asc_nulls_last());
        Dataset<Row> minTimePlan = tableData.withColumn("time_rank", row_number().over(w)).where(col("time_rank").equalTo(1)).drop("time_rank")
                .selectExpr("start_city_id","start_city_name","transfer_city_id as transfer_min_time_id","transfer_city_name as transfer_min_time_name","end_city_id","end_city_name","first_traffic_type","second_traffic_type","total_time");
        return minTimePlan;
    }

    private void saveRedis(Dataset<Row> dataSet) {
        dataSet.foreachPartition(
                data -> {
                    Jedis jedis = new Jedis("localhost");
                    SetParams setParams=new SetParams().ex(RedisConfig.REDIS_KEY_ALIVE);
                    while (data.hasNext()) {
                        Row record = data.next();
                        String key = String.format("cityMinPriceTime:%s@%s@%s%s", record.<Integer>getAs("start_city_id"), record.<Integer>getAs("end_city_id"),record.<String>getAs("first_traffic_type"),record.<String>getAs("second_traffic_type"));
                        PlanMinPriceTimeBO planMinPriceTimeBO=new PlanMinPriceTimeBO();
                        Integer minPriceCityId=record.<Integer>getAs("transfer_min_price_id");
                        if(minPriceCityId!=null){
                            planMinPriceTimeBO.setMinPriceCityId(minPriceCityId);
                            planMinPriceTimeBO.setMinTotalPrice(record.<Double>getAs("total_price"));
                        }
                        Integer minTimeCityId=record.<Integer>getAs("transfer_min_time_id");
                        if(minTimeCityId!=null){
                            planMinPriceTimeBO.setMinTimeCityId(minTimeCityId);
                            planMinPriceTimeBO.setMinTotalTime(record.<Integer>getAs("total_time"));
                        }
                        String value = JSONObject.toJSONString(planMinPriceTimeBO);;
                        if (isLocalEnvConfig()) {
                            System.out.println("key:" + key + ",value:" + value);
                        }
                        jedis.set(key,  value, setParams);
                    }
                }
        );
    }

    public static void main(String[] args) throws IOException {
        PlanMinPriceTime planMinPriceTime =new PlanMinPriceTime();
        planMinPriceTime.runAll(args,true);
    }
}

package com.demo.analysis.redis;

import com.alibaba.fastjson.JSON;
import com.demo.base.AbstractSparkSql;
import com.demo.bean.es.TicketRank;
import com.demo.common.ColumnUtil;
import com.demo.conf.RedisConfig;
import com.demo.util.KafkaUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;
import scala.collection.Seq;

import java.io.IOException;

import static org.apache.spark.sql.functions.*;

/**
 * 余票分级缓存：城市查询线路和订单 比 数据存入es
 */
public class TicketCacheTime extends AbstractSparkSql {

    public static void main(String[] args) throws IOException {
        TicketCacheTime ticketCacheTime = new TicketCacheTime();
        ticketCacheTime.runAll(args, true, true);
    }

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        Dataset<Row> tableData=trainTicketRank(spark,args);
        saveRedis(tableData);
        sendKafkaMessage();
    }

    private Dataset<Row> trainTicketRank(SparkSession spark,String[] args ) {
        Dataset<Row> trainSearchData = spark.sql("SELECT start_city_name, end_city_name,traffic_type,start_station_name, " +
                "end_station_name,start_station_code,end_station_code,search_num FROM app_trafficwisdom.stats_train_search WHERE search_num >= 100");
        Dataset<Row> trainOrderData = spark.sql("SELECT start_station_code, end_station_code, traffic_type, order_num FROM app_trafficwisdom.stats_train_direct_order");
        Seq<String> seqStation = ColumnUtil.columnNames("start_station_code,end_station_code,traffic_type");
        Seq<String> naFillZero=ColumnUtil.columnNames("order_num");
        Dataset<Row> trainRank = trainSearchData.join(trainOrderData, seqStation,"left").na().fill(0,naFillZero)
                .withColumn("order_percent", round(col("order_num").divide(col("search_num")), 6))
                .withColumn("search_rank", row_number().over(Window.partitionBy("traffic_type").orderBy(col("search_num").desc_nulls_last())))
                .withColumn("order_rank", row_number().over(Window.partitionBy("traffic_type").orderBy(col("order_percent").desc_nulls_last()))).repartition(1);
        Dataset<Row> maxRank = trainRank.groupBy("traffic_type").agg(functions.max("search_rank").as("totalSize"));
        Dataset<Row> joinResult = trainRank.join(maxRank, "traffic_type");
        return joinResult;
    }

    private void saveRedis(Dataset<Row> dataSet) {
        dataSet.foreachPartition(
                data -> {
                    Jedis jedis = new Jedis("localhost");
                    SetParams setParams=new SetParams().ex(RedisConfig.REDIS_KEY_ALIVE);
                    while (data.hasNext()) {
                        Row record = data.next();
                        String key = String.format("ticketRank:" + "%s@%s@%s@%s@%s", record.<String>getAs("start_city_name"), record.<String>getAs("start_station_code"), record.<String>getAs("end_city_name"), record.<String>getAs("end_station_code"), record.<String>getAs("traffic_type"));
                        TicketRank ri = new TicketRank(record.<Integer>getAs("search_rank"), record.<Integer>getAs("order_rank"), record.<Integer>getAs("totalSize"));
                        String value = JSON.toJSONString(ri);
                        jedis.set(key,  value, setParams);
                    }
                }
        );
    }

    public void sendKafkaMessage() {
        String type="ticket_rank";
        KafkaUtils.sendFinishNotification(type);
    }
}

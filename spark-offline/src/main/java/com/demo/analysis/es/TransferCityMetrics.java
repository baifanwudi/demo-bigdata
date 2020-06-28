package com.demo.analysis.es;

import com.demo.base.AbstractReadDiffEnvTable;
import com.demo.conf.RedisConfig;
import com.demo.operation.TransferCityMapFunction;
import com.demo.util.ElasticSearchOperation;
import com.demo.util.KafkaUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.client.RestHighLevelClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.io.IOException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_set;

public class TransferCityMetrics extends AbstractReadDiffEnvTable {

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        Dataset<Row> tableData = readTable(spark, args);
        ExpressionEncoder<Row> encoder = getEncoder();
        Dataset<Row> combineData = tableData.groupBy("start_city_id", "end_city_id", "first_traffic_type", "second_traffic_type", "distance")
                .agg(collect_set(col("transfer_city_id")).as("combined_set"));
        Dataset<Row> gzipData = combineData.map(new TransferCityMapFunction(), encoder).repartition(5);
        saveRedis(gzipData);
        saveEs(gzipData);
        sendKafkaMessage();
    }

    private ExpressionEncoder<Row> getEncoder() {
        StructType structTypeMember = new StructType();
        structTypeMember = structTypeMember.add("start_city_id", DataTypes.IntegerType, true);
        structTypeMember = structTypeMember.add("end_city_id", DataTypes.IntegerType, true);
        structTypeMember = structTypeMember.add("first_traffic_type", DataTypes.StringType, true);
        structTypeMember = structTypeMember.add("second_traffic_type", DataTypes.StringType, true);
        structTypeMember = structTypeMember.add("transfer_city_info", DataTypes.StringType, true);
        ExpressionEncoder<Row> encoder = RowEncoder.apply(structTypeMember);
        return encoder;
    }

    private void saveRedis(Dataset<Row> dataSet) {
        dataSet.foreachPartition(
                data -> {
                    Jedis jedis = new Jedis("localhost");
                    SetParams setParams=new SetParams().ex(RedisConfig.REDIS_KEY_ALIVE);
                    while (data.hasNext()) {
                        Row record = data.next();
                        String key = String.format("transferCityMetrics:%s@%s@%s", record.<Integer>getAs("start_city_id"), record.<Integer>getAs("end_city_id"), record.<String>getAs("first_traffic_type") + record.<String>getAs("second_traffic_type"));
                        String value = record.<String>getAs("transfer_city_info");
                        if (isLocalEnvConfig()) {
                            System.out.println("key:" + key + ",value:" + value);
                        }
                        jedis.set(key,  value, setParams);
                    }
                }
        );
    }

    private void saveEs(Dataset<Row> dataSet) throws IOException {
        ElasticSearchOperation elasticSearchOperation=new ElasticSearchOperation();
        RestHighLevelClient client=elasticSearchOperation.getRestHighLevelClient();
        String middleIndex = "transfer_city_metrics";
        String type = "route";
        String source = "{\n" +
                "    \"route\": {\n" +
                "      \"properties\": {\n" +
                "        \"start_city_id\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"end_city_id\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"first_traffic_type\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"second_traffic_type\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"transfer_city_info\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }";
        logger.info("es source is :" + source);
        elasticSearchOperation.processEsAll(dataSet, client, middleIndex, source, type);
    }

    /**
     * SELECT start_city_id, transfer_city_id, end_city_id, first_traffic_type, second_traffic_type, distance FROM mid_trafficwisdom.city_transfer_info WHERE start_city_name = '喀什市' AND end_city_name = '许昌市' LIMIT 10000
     * @param spark
     * @return
     */
    @Override
    protected Dataset<Row> offlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.read()
                .option("inferschema", "true")
                .option("header", "true")
                .option("encoding", "gbk")
                .csv("D:\\data\\csv\\city_transfer_info.csv");
        tableData.show(false);
        tableData.printSchema();
        return tableData;
    }

    @Override
    protected Dataset<Row> onlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.sql("SELECT start_city_id, transfer_city_id, end_city_id, first_traffic_type, second_traffic_type, distance FROM mid_trafficwisdom.city_transfer_info");
        return tableData;
    }

    public void sendKafkaMessage() {
        String type = "transfer_city_suc";
        KafkaUtils.sendFinishNotification(type);
    }

    public static void main(String[] args) throws IOException {
        TransferCityMetrics transferCityMetrics = new TransferCityMetrics();
        transferCityMetrics.runAll(args, true, true);
    }
}

package com.demo.analysis.es;

import com.alibaba.fastjson.JSONObject;
import com.demo.base.AbstractReadDiffEnvTable;
import com.demo.bean.es.CityMetrics;
import com.demo.conf.RedisConfig;
import com.demo.util.ElasticSearchOperation;
import com.demo.util.KafkaUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.client.RestHighLevelClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.io.IOException;

public class SectionCityMetrics extends AbstractReadDiffEnvTable {

    public static void main(String[] args) throws IOException {
        SectionCityMetrics cityMetrics = new SectionCityMetrics();
        cityMetrics.runAll(args, true, true);
    }

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        Dataset<Row> tableData = readTable(spark, args).drop("start_city_name","end_city_name","create_date");
        tableData.cache();
        saveRedis(tableData);
        saveEs(tableData);
        sendKafkaMessage();
        tableData.unpersist();
    }

    private void saveRedis(Dataset<Row> dataSet) {
        dataSet.foreachPartition(
                data -> {
                    Jedis jedis = new Jedis("localhost");
                    SetParams setParams=new SetParams().ex(RedisConfig.REDIS_KEY_ALIVE);
                    while (data.hasNext()) {
                        Row record = data.next();
                        String key = String.format("cityMetrics:%s@%s", record.<Integer>getAs("start_city_id"), record.<Integer>getAs("end_city_id"));
                        Double distance = record.<Double>getAs("distance");
                        Integer trainCount = record.<Integer>getAs("train_count");
                        Double highRailwayPercent = record.<Double>getAs("high_railway_percent");
                        Integer trainAvgTime = record.<Integer>getAs("train_avg_time");
                        Integer trainMinTime = record.<Integer>getAs("train_min_time");
                        Integer trainMaxTime = record.<Integer>getAs("train_max_time");
                        Double trainAvgPrice = record.<Double>getAs("train_avg_price");
                        Double trainMinPrice = record.<Double>getAs("train_min_price");
                        Double trainMaxPrice = record.<Double>getAs("train_max_price");
                        Integer flightCount = record.<Integer>getAs("flight_count");
                        Integer flightAvgTime = record.<Integer>getAs("flight_avg_time");
                        Integer flightMinTime = record.<Integer>getAs("flight_min_time");
                        Integer flightMaxTime = record.<Integer>getAs("flight_max_time");
                        Double flightAvgPrice = record.<Double>getAs("flight_avg_price");
                        Double flightMinPrice = record.<Double>getAs("flight_min_price");
                        Double flightMaxPrice = record.<Double>getAs("flight_max_price");
                        Integer busCount = record.<Integer>getAs("bus_count");
                        Integer busAvgTime = record.<Integer>getAs("bus_avg_time");
                        Integer busMinTime = record.<Integer>getAs("bus_min_time");
                        Integer busMaxTime = record.<Integer>getAs("bus_max_time");
                        Double busAvgPrice = record.<Double>getAs("bus_avg_price");
                        Double busMinPrice = record.<Double>getAs("bus_min_price");
                        Double busMaxPrice = record.<Double>getAs("bus_max_price");

                        CityMetrics cityMetrics = CityMetrics.builder().distance(distance).trainCount(trainCount).highRailwayPercent(highRailwayPercent).trainAvgTime(trainAvgTime)
                                .trainMinTime(trainMinTime).trainMaxTime(trainMaxTime).trainAvgPrice(trainAvgPrice).trainMinPrice(trainMinPrice).trainMaxPrice(trainMaxPrice)
                                .flightCount(flightCount).flightAvgTime(flightAvgTime).flightMinTime(flightMinTime).flightMaxTime(flightMaxTime).flightAvgPrice(flightAvgPrice)
                                .flightMinPrice(flightMinPrice).flightMaxPrice(flightMaxPrice).busCount(busCount).busAvgTime(busAvgTime).busMinTime(busMinTime).busMaxTime(busMaxTime)
                                .busAvgPrice(busAvgPrice).busMinPrice(busMinPrice).busMaxPrice(busMaxPrice).build();
                        String value = JSONObject.toJSONString(cityMetrics);
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
        String middleIndex = "section_city_metrics";
        String type = "route";
        String source = "{\n" +
                "      \"properties\": {\n" +
                "        \"start_city_id\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"end_city_id\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"distance\": {\n" +
                "          \"type\": \"double\"\n" +
                "        },\n" +
                "        \"train_count\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"high_railway_percent\": {\n" +
                "          \"type\": \"double\"\n" +
                "        },\n" +
                "        \"train_avg_time\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"train_min_time\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"train_max_time\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"train_avg_price\": {\n" +
                "          \"type\": \"double\"\n" +
                "        },\n" +
                "        \"train_min_price\": {\n" +
                "          \"type\": \"double\"\n" +
                "        },\n" +
                "        \"train_max_price\": {\n" +
                "          \"type\": \"double\"\n" +
                "        },\n" +
                "        \"flight_count\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"flight_avg_time\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"flight_min_time\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"flight_max_time\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"flight_avg_price\": {\n" +
                "          \"type\": \"double\"\n" +
                "        },\n" +
                "        \"flight_min_price\": {\n" +
                "          \"type\": \"double\"\n" +
                "        },\n" +
                "        \"flight_max_price\": {\n" +
                "          \"type\": \"double\"\n" +
                "        },\n" +
                "        \"bus_count\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"bus_avg_time\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"bus_min_time\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"bus_max_time\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"bus_avg_price\": {\n" +
                "          \"type\": \"double\"\n" +
                "        },\n" +
                "        \"bus_min_price\": {\n" +
                "          \"type\": \"double\"\n" +
                "        },\n" +
                "        \"bus_max_price\": {\n" +
                "          \"type\": \"double\"\n" +
                "        }\n" +
                "      }\n" +
                "    }";
        logger.info("es source is :" + source);
        elasticSearchOperation.processEsAll(dataSet, client, middleIndex, source, type);
    }

    @Override
    protected Dataset<Row> onlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.sql("select * from  app_trafficwisdom.section_city_metrics ");
        return tableData;
    }

    /**
     * @param spark
     * @return
     */
    @Override
    protected Dataset<Row> offlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.read()
                .option("inferschema", "true")
                .option("header", "true")
                .option("encoding", "gbk")
                .csv("D:\\data\\csv\\section_city_metrics.csv");
        tableData.show(false);
        tableData.printSchema();
        return tableData;
    }

    public void sendKafkaMessage() {
        String type = "city_through";
        KafkaUtils.sendFinishNotification(type);
    }
}

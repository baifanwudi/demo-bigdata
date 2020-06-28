package com.demo.analysis.es;

import com.demo.base.AbstractReadDiffEnvTable;
import com.demo.util.ElasticSearchOperation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import java.io.IOException;
import java.util.Calendar;

/**
 * 节假日
 */
public class HolidayDate extends AbstractReadDiffEnvTable {

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        Dataset<Row> tableData = readTable(spark, args).selectExpr("create_date as date", "isholiday as is_holiday");
        saveEs(tableData);
    }

    @Override
    protected Dataset<Row> onlineTable(SparkSession spark, String[] args) {
        Calendar calendar = Calendar.getInstance();
        Integer year = calendar.get(Calendar.YEAR);
        Dataset<Row> tableData = spark.sql("SELECT * FROM dim_traffic.dim_date where datekey like '" + year + "%'");
        return tableData;
    }

    /**
     * SELECT create_date,isholiday FROM dim_traffic.dim_date where datekey like '2019%'LIMIT 10000
     */
    @Override
    protected Dataset<Row> offlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.read()
                .option("inferschema", "true")
                .option("header", "true")
                .option("encoding", "gbk")
                .csv("D:\\data\\csv\\dim_date.csv").selectExpr("substring(cast(create_date as string),1,10) as create_date", "isholiday");
        tableData.printSchema();
        tableData.show(false);
        return tableData;
    }

    protected void saveEs(Dataset<Row> dataSet) throws IOException {
        ElasticSearchOperation elasticSearchOperation = new ElasticSearchOperation() {
            @Override
            public void createEsIndex(RestHighLevelClient client, String esIndex, String esType, String source) throws IOException {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(esIndex);
                //创建分片数量和副本数量
                createIndexRequest.settings(Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                );
                createIndexRequest.mapping(esType, source, XContentType.JSON);
                CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest);
                logger.info("建立index:" + esType + "是否成功" + createIndexResponse.isAcknowledged());
            }

        };
        RestHighLevelClient client = elasticSearchOperation.getRestHighLevelClient();
        String middleIndex = "holiday_date";
        String type = "route";
        String source = "{\n" +
                "      \"properties\": {\n" +
                "        \"date\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"is_holiday\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        }\n" +
                "      }\n" +
                "    }";
        logger.info("es source is :" + source);
        elasticSearchOperation.processEsAll(dataSet, client, middleIndex, source, type);
    }

    public static void main(String[] args) throws IOException {
        HolidayDate searchHotCity = new HolidayDate();
        searchHotCity.runAll(args, true, true);
    }
}

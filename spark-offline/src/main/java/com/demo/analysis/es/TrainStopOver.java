package com.demo.analysis.es;

import com.demo.base.AbstractReadDiffEnvTable;
import com.demo.util.ElasticSearchOperation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.client.RestHighLevelClient;
import java.io.IOException;

public class TrainStopOver extends AbstractReadDiffEnvTable {

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        Dataset<Row> tableData = readTable(spark,args);
        saveEs(tableData);
    }

    private void saveEs(Dataset<Row> dataSet) throws IOException {
        ElasticSearchOperation elasticSearchOperation=new ElasticSearchOperation();
        RestHighLevelClient client=elasticSearchOperation.getRestHighLevelClient();
        String middleIndex = "train_stop_over";
        String type = "train";
        String source = "{\n" +
                "      \"properties\": {\n" +
                "        \"train_no\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"transport_code\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"station_sequence\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"station_name\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"arrive_time\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"leave_time\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"stopover_time\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"date\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"station_code\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"after_day\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        }\n" +
                "      }\n" +
                "    }";
        logger.info("es source is :" + source);
        elasticSearchOperation.processEsAll(dataSet, client, middleIndex, source, type);
    }

    @Override
    protected Dataset<Row> onlineTable(SparkSession spark, String[] args){
        Dataset<Row> tableData= spark.sql("select train_no,transport_code,station_code,station_name,station_sequence,duration_date as date,arrive_time,leave_time,sleep_time as stopover_time,after_day from mid_trafficwisdom.train_stopover_split").distinct();
        return tableData;
    }

    @Override
    protected Dataset<Row> offlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.read()
                .option("inferschema", "true")
                .option("header", "true")
                .option("encoding", "gbk")
                .csv("/Users/allenbai/Downloads/data/csv/train_stopover_split.csv")
                .selectExpr("train_no","transport_code","station_code","station_name","station_sequence",
                        "substring(cast(duration_date as string),1,10) as date","arrive_time","leave_time","sleep_time as stopover_time","after_day");
        tableData.printSchema();
        tableData.show(false);
        return tableData;
    }

    public static void main(String[] args) throws IOException {
        TrainStopOver trainStopOver = new TrainStopOver();
        trainStopOver.runAll(args, true, true);
    }
}


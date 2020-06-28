package com.demo.analysis.es;

import com.demo.base.AbstractSparkSql;
import com.demo.util.ElasticSearchOperation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.client.RestHighLevelClient;
import java.io.IOException;

/**
 *  火车站到站，可以到达站点，包含经纬度
 */
public class StationSectionLnglat extends AbstractSparkSql {

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        Dataset<Row> tableData=onlineTable(spark);
        saveEs(tableData);
    }

    private Dataset<Row> onlineTable(SparkSession spark){
        Dataset<Row> tableData=spark.sql("SELECT A.start_station_code, A.start_station_name, CAST(B.lng AS string) AS start_lng, CAST(B.lat AS string) AS start_lat, " +
                "end_station_code, end_station_name, CAST(C.lng AS string) AS end_lng, CAST(C.lat AS string) AS end_lat FROM " +
                "(SELECT start_station_code, start_station_name, end_station_code, end_station_name FROM app_trafficwisdom.section_traffic " +
                "WHERE traffic_type = 'T' GROUP BY start_station_code, start_station_name, end_station_code, end_station_name ) A " +
                "JOIN base_tcdctrafficwisdomwukongbase.station_train_base B ON A.start_station_code = B.station_code " +
                "JOIN base_tcdctrafficwisdomwukongbase.station_train_base C ON A.end_station_code = C.station_code");
        return tableData;
    }

    private void saveEs(Dataset<Row> dataSet) throws IOException {
        ElasticSearchOperation elasticSearchOperation=new ElasticSearchOperation();
        RestHighLevelClient client=elasticSearchOperation.getRestHighLevelClient();
        String middleIndex = "station_section_lnglat";
        String type = "route";
        String source = "{\n" +
                "      \"properties\": {\n" +
                "        \"start_station_code\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"start_station_name\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"start_lng\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"start_lat\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"end_station_name\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"end_station_code\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"end_lng\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"end_lat\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        }\n" +
                "      }\n" +
                "    }";
        logger.info("es source is :"+source);
        elasticSearchOperation.processEsAll(dataSet, client, middleIndex, source, type);
    }

    public static void main(String[] args) throws IOException {
        StationSectionLnglat stationSectionLnglat =new StationSectionLnglat();
        stationSectionLnglat.runAll(args,true,true);
    }
}

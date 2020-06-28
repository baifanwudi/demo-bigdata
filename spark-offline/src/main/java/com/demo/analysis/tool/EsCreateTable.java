package com.demo.analysis.tool;

import com.demo.base.AbstractSparkSql;
import com.demo.util.ElasticSearchOperation;
import com.demo.conf.ESConfig;
import com.demo.util.DateUtils;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class EsCreateTable extends AbstractSparkSql {

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        saveEs();
    }

    private void saveEs() throws IOException {
        ElasticSearchOperation elasticSearchOperation=new ElasticSearchOperation();
        RestHighLevelClient client=elasticSearchOperation.getRestHighLevelClient();
        String middleIndex = "transfer_detail_log";
        String esIndex = ESConfig.PRE_INDEX + "." + middleIndex + "." + DateUtils.nowDate();
        String type = "route";
        String source = "{\n" +
                "      \"properties\": {\n" +
                "        \"plan_id\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        },\n" +
                "        \"value\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "        }\n" +
                "      }\n" +
                "    }";
        logger.info("es source is :" + source);
        elasticSearchOperation.deleteIfExistEsIndex( client, esIndex);
        elasticSearchOperation.createEsIndex(client, esIndex, type, source);

        client.close();
    }

    public static void main(String[] args) throws IOException {
        EsCreateTable esCreateTable = new EsCreateTable();
        esCreateTable.runAll(args, true, true);
    }
}

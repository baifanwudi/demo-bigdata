package com.demo.analysis.tool;

import com.demo.base.AbstractSparkSql;
import com.demo.util.ElasticSearchOperation;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * ES数据滚回，把老的索引名关联到别名下
 */
public class ConnectOldIndexToAlias extends AbstractSparkSql {

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        String oldIndex=args[0];
        String aliasIndex=args[1];
        logger.info("将老的索引:"+oldIndex+"关联到别名:"+aliasIndex);
        ElasticSearchOperation elasticSearchOperation=new ElasticSearchOperation();
        RestHighLevelClient client=elasticSearchOperation.getRestHighLevelClient();
        try {
            elasticSearchOperation.createIndexAlias(client, oldIndex, aliasIndex);
            elasticSearchOperation.deleteIfExistOldIndex(client, oldIndex, aliasIndex);
            logger.info("修改完毕");
        }finally {
            client.close();
        }
    }

    public static void main(String[] args) throws IOException {
        ConnectOldIndexToAlias connectOldIndexToAlias =new ConnectOldIndexToAlias();
        connectOldIndexToAlias.runAll(args,false,true);
    }
}

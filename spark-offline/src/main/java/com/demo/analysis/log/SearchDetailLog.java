package com.demo.analysis.log;

import com.demo.base.AbstractSparkSql;
import com.demo.util.DateUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SearchDetailLog extends AbstractSparkSql {

    public static final String SEARCH_DETAIL_LOG_PATH= "viewfs:///data/twms/traffichuixing/wukong_transfer_jlh";

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        String date = DateUtils.produceDateOrYesterday(args);
        String path=SEARCH_DETAIL_LOG_PATH+"/"+date;
        Dataset<Row> searchDetail=spark.read().schema(tableSchema()).json(path).distinct().repartition(1);
        searchDetail.createOrReplaceTempView("searchDetailLog");
        String sql=" insert overwrite table tmp_trafficwisdom.search_detail_log partition(log_date='"+date+"') select interruptCode,interruptMsg,requestId,transferType from searchDetailLog";
        logger.info("executing sql is :"+sql);
        spark.sql(sql);
    }

    private StructType tableSchema(){
        List<StructField> inputFields=new ArrayList<>();
        String splitSeq=",";
        String stringType="interruptCode,interruptMsg,requestId,transferType";
        for(String stringTmp:stringType.split(splitSeq)){
            inputFields.add(DataTypes.createStructField(stringTmp,DataTypes.StringType,true));
        }
        return DataTypes.createStructType(inputFields);
    }

    public static void main(String[] args) throws IOException {
        SearchDetailLog searchDetailLog =new SearchDetailLog();
        searchDetailLog.runAll(args,true);
    }
}

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

public class TransferPlanLog extends AbstractSparkSql {

    public static final String TRANSFER_LOG_PATH = "viewfs:///data/twms/traffichuixing/transferplan_show_record_product";

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        String date = DateUtils.produceDateOrYesterday(args);
        executeHive(spark, date);
    }

    private void executeHive(SparkSession spark, String date) {
        String path = TRANSFER_LOG_PATH + "/" + date;
        Dataset<Row> transferLog = spark.read().schema(tableSchema()).json(path).distinct().repartition(10);
        transferLog.createOrReplaceTempView("transferLog");
        String sql = " insert overwrite table tmp_trafficwisdom.transfer_plan_log_tmp partition(log_date='" + date + "') select unionId,itemId,planId,action,transferType," +
                "from_unixtime(cast(substring(time,1,10) as int),'yyyy-MM-dd HH:mm:ss') as creat_time,rankIndex as rank_index from transferLog ";
        logger.info("executing sql is :" + sql);
        spark.sql(sql);
    }

    private StructType tableSchema() {
        List<StructField> inputFields = new ArrayList<>();
        String splitSeq = ",";
        String stringType = "action,itemId,time,unionId,planId,transferType";
        String integerType = "rankIndex";
        for (String stringTmp : stringType.split(splitSeq)) {
            inputFields.add(DataTypes.createStructField(stringTmp, DataTypes.StringType, true));
        }
        inputFields.add(DataTypes.createStructField(integerType, DataTypes.IntegerType, true));
        return DataTypes.createStructType(inputFields);
    }

    public static void main(String[] args) throws IOException {
        TransferPlanLog transferPlanLog = new TransferPlanLog();
        transferPlanLog.runAll(args, true);
    }
}


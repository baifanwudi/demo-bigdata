package com.demo.operation;

import com.alibaba.fastjson.JSONObject;
import com.demo.bean.es.TransferCityInfo;
import com.demo.bean.es.TransferCityMetricBO;
import com.demo.common.GZipUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.List;

public class TransferCityMapFunction implements MapFunction<Row, Row> {

    @Override
    public Row call(Row row) throws Exception {
        Integer startCityId = row.<Integer>getAs("start_city_id");
        Integer endCityId = row.<Integer>getAs("end_city_id");
        String firstTrafficType = row.<String>getAs("first_traffic_type");
        String secondTrafficType = row.<String>getAs("second_traffic_type");
        Double distance = row.<Double>getAs("distance");
        WrappedArray<Integer> rowWrappedArray = row.<WrappedArray<Integer>>getAs("combined_set");
        List<TransferCityInfo> transferCityInfos = new ArrayList<TransferCityInfo>();
        List<Integer> rowList = new ArrayList<Integer>(JavaConversions.<Integer>seqAsJavaList(rowWrappedArray));
        for (Integer transferCityId : rowList) {

            TransferCityInfo transferCityInfo = new TransferCityInfo();
            transferCityInfo.setTransferCityId(transferCityId);
            transferCityInfos.add(transferCityInfo);
        }
        TransferCityMetricBO transferCityMetricBO = new TransferCityMetricBO();
        transferCityMetricBO.setDistance(distance);
        transferCityMetricBO.setTransferCityInfos(transferCityInfos);
        String value = GZipUtils.gzip(JSONObject.toJSONString(transferCityMetricBO));
//        String value = JSONObject.toJSONString(transferCityMetricBO);
        return RowFactory.create(startCityId, endCityId, firstTrafficType, secondTrafficType, value);
    }
}

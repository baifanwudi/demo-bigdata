package com.demo.operation;

import com.demo.util.DesUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class TrainSeatMapFunction implements MapFunction<Row, Row> {

    @Override
    public Row call(Row row) throws Exception {
        String startCityName = row.<String>getAs("start_city_name");
        String endCityName = row.<String>getAs("end_city_name");
        String transferCityName = row.<String>getAs("transfer_city_name");
        String startStationCode = row.<String>getAs("start_station_code");
        String transferArriveStationCode = row.<String>getAs("transfer_arrive_station_code");
        String firstTrafficType = row.<String>getAs("first_traffic_type");
        Integer firstSeatType=row.<Integer>getAs("first_seat_type");
        String transferLeaveStationCode = row.<String>getAs("transfer_leave_station_code");
        String endStationCode = row.<String>getAs("end_station_code");
        String secondTrafficType = row.<String>getAs("second_traffic_type");
        Integer secondSeatType=row.<Integer>getAs("second_seat_type");
        String firstTrafficCode = row.<String>getAs("first_traffic_code");
        String secondTrafficCode = row.<String>getAs("second_traffic_code");
        Long totalNum = row.<Long>getAs("total_num");
        String itemId = DesUtils.encoderByMd5(new StringBuffer().append(startCityName).append(endCityName).append(transferCityName).append(startStationCode).append(transferArriveStationCode).append(firstTrafficType).append(transferLeaveStationCode)
                .append(endStationCode).append(secondTrafficType).append(firstTrafficCode).append(secondTrafficCode).toString());
        return RowFactory.create(itemId, firstSeatType, secondSeatType, totalNum);
    }
}

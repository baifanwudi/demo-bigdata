package com.demo.bean.spider;

import lombok.Data;

@Data
public class TrainStopoverSplitBO {

    private String trainNo;

    private String transportCode;

    private String stationName;

    private Integer stationSequence;

    private String durationDate;

    private String arriveTime;

    private String leaveTime;

    private Integer sleepTime;

    private Integer afterDay;
}

package com.demo.bean.redis;

import lombok.Data;

@Data
public class TrainStopOverBO {

    private String stationCode;

    private String stationName;

    private Integer rank;

    private Integer diffSequence;

    private Integer diffRunTime;
}

package com.demo.bean.spider;

import lombok.Builder;
import lombok.Data;


@Data
@Builder
public class TrainSectionBO {

    private String trainNo;

    private String transportCode;

    private String durationDate;

    private Integer beginId;

    private String beginStationName;

    private String beginTime;

    private Integer endId;

    private String endStationName;

    private String endTime;
}

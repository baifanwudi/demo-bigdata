package com.demo.bean.spider;

import lombok.Builder;
import lombok.Data;


@Data
@Builder
public class BusSectionBO {

    private String busNo;

    private String durationDate;

    private Integer startCityId;

    private Integer startBusCityId;

    private String startBusCityName;

    private String startStationName;

    private String startTime;

    private Integer endCityId;

    private Integer endBusCityId;

    private String endBusCityName;

    private String endStationName;

    private String endTime;

    private Long runTime;

    private Double ticketPrice;

    private Double distance;
}

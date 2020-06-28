package com.demo.bean.es;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class CityMetrics {

    private Double distance;

    private Integer trainCount;

    private Double highRailwayPercent;

    private Integer trainAvgTime;

    private Integer trainMinTime;

    private Integer trainMaxTime;

    private Double trainAvgPrice;

    private Double trainMinPrice;

    private Double trainMaxPrice;

    private Integer flightCount;

    private Integer flightAvgTime;

    private Integer flightMinTime;

    private Integer flightMaxTime;

    private Double flightAvgPrice;

    private Double flightMinPrice;

    private Double flightMaxPrice;

    private Integer busCount;

    private Integer busAvgTime;

    private Integer busMinTime;

    private Integer busMaxTime;

    private Double busAvgPrice;

    private Double busMinPrice;

    private Double busMaxPrice;
}

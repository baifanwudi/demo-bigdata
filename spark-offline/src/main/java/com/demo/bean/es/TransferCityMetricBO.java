package com.demo.bean.es;

import lombok.Data;

import java.util.List;

@Data
public class TransferCityMetricBO {

    private Double distance;

    private List<TransferCityInfo> transferCityInfos;
}

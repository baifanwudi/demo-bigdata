package com.demo.bean.redis;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SeatTypeRank {

    private Integer firstSeatType;

    private Integer secondSeatType;

    private Long totalNum;

    private Integer rankNum;
}

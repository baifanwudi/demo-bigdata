package com.demo.bean.es;

import lombok.Data;

@Data
public class TicketRank {
    //搜索占比
    private Integer searchRank;
    //订单转化占比
    private Integer orderRank;
    //总数量
    private Integer totalSize;

    public TicketRank(Integer searchRank, Integer orderRank, Integer totalSize) {
        this.searchRank = searchRank;
        this.orderRank = orderRank;
        this.totalSize = totalSize;
    }

    @Override
    public String toString() {
        return "RankInfo{" +
                "searchRank=" + searchRank +
                ", orderRank=" + orderRank +
                ", totalSize=" + totalSize +
                '}';
    }
}

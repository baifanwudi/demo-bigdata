package com.demo.util;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hw41838 on 2017/3/29.
 */
public final class MapDistanceUtils {

    private static double EARTH_RADIUS = 6378.393;

    private static final double rad(double d) {
        return d * Math.PI / 180.0;
    }

    /**
     * 根据两个位置的经纬度，来计算两地的距离（单位为M）
     * 参数为String类型
     *
     * @param lat1Str 用户经度
     * @param lng1Str 用户纬度
     * @param lat2Str 商家经度
     * @param lng2Str 商家纬度
     * @return
     */
    public static double getDistance(String lat1Str, String lng1Str, String lat2Str, String lng2Str) {
        double lat1 = Double.parseDouble(lat1Str);
        double lng1 = Double.parseDouble(lng1Str);
        double lat2 = Double.parseDouble(lat2Str);
        double lng2 = Double.parseDouble(lng2Str);
        return getDistance(lat1, lng1, lat2, lng2);
    }

    public static double getDistance(double lat1, double lng1, double lat2, double lng2) {
        double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double difference = radLat1 - radLat2;
        double mdifference = rad(lng1) - rad(lng2);
        double distance = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(difference / 2), 2)
                + Math.cos(radLat1) * Math.cos(radLat2)
                * Math.pow(Math.sin(mdifference / 2), 2)));
        distance = distance * EARTH_RADIUS;
        distance = Math.round(distance * 10000) / 10000;
        return distance * 1000;
    }

    public static double getDistance_KM(BigDecimal lat1Str, BigDecimal lng1Str, BigDecimal lat2Str, BigDecimal lng2Str) {
        double lat1 = lat1Str.doubleValue();
        double lng1 = lng1Str.doubleValue();
        double lat2 = lat2Str.doubleValue();
        double lng2 = lng2Str.doubleValue();
        //公里
        return getDistance(lat1, lng1, lat2, lng2) / 1000d;
    }

    /**
     * 获取当前用户一定距离以内的经纬度值
     * 单位米 return minLat
     * 最小经度 minLng
     * 最小纬度 maxLat
     * 最大经度 maxLng
     * 最大纬度 minLat
     */
    public static Map getAround(String latStr, String lngStr, String radius) {
        Map map = new HashMap();
        // 传值给经度
        Double latitude = Double.parseDouble(latStr);
        // 传值给纬度
        Double longitude = Double.parseDouble(lngStr);
        // 获取每度
        Double degree = (24901 * 1609) / 360.0;
        double raidusMile = Double.parseDouble(radius);
        Double mpdLng = Double.parseDouble((degree * Math.cos(latitude * (Math.PI / 180)) + "").replace("-", ""));
        Double dpmLng = 1 / mpdLng;
        Double radiusLng = dpmLng * raidusMile;
        //获取最小经度
        Double minLat = longitude - radiusLng;
        // 获取最大经度
        Double maxLat = longitude + radiusLng;
        Double dpmLat = 1 / degree;
        Double radiusLat = dpmLat * raidusMile;
        // 获取最小纬度
        Double minLng = latitude - radiusLat;
        // 获取最大纬度
        Double maxLng = latitude + radiusLat;
        map.put("minLat", minLat + "");
        map.put("maxLat", maxLat + "");
        map.put("minLng", minLng + "");
        map.put("maxLng", maxLng + "");
        return map;
    }

}

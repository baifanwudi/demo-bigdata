package com.demo.demo;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.ArrayList;
import java.util.List;

/**
 * 防止线下ES崩溃
 */
public class EsDataOffline {

    public static RestHighLevelClient getRestHighLevelClient() {
        String[] ips = "10.100.203.74".split(",");
        Integer port = 60000;
        List<HttpHost> list = new ArrayList<HttpHost>();
        for (String ip : ips) {
            list.add(new HttpHost(ip, port, "http"));
        }
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(list.toArray(new HttpHost[list.size()])));
        return client;
    }
}

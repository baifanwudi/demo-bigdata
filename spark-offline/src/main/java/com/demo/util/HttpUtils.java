package com.demo.util;


import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by hw41838 on 2017/2/21.
 */
public class HttpUtils {

    /**
     * 发送get请求
     *
     * @param url
     * @param closeableHttpClient
     * @return
     * @throws IOException
     */
    private static Logger log = LoggerFactory.getLogger(HttpUtils.class);

    public static String get(String url, CloseableHttpClient closeableHttpClient) throws IOException {
        String content = "";
        HttpGet httpGet = new HttpGet(url);
        // 执行get请求
        HttpResponse httpResponse = closeableHttpClient.execute(httpGet);
        int statusCode = httpResponse.getStatusLine().getStatusCode();
        HttpEntity entity = httpResponse.getEntity();
        if (entity != null) {
            content = EntityUtils.toString(entity);
        }
        if (statusCode != HttpStatus.SC_OK) {
            log.error("返回结果：" + content);
            content = "";
        }
        httpGet.abort();
        return content;
    }

    /**
     * 火火，火汽调用高德地图接口
     * 发送get请求
     *
     * @param url
     * @return
     * @throws IOException
     */
    public static String doGet(String url) throws IOException {
        String content = "";
        HttpGet httpGet = new HttpGet(url);
        // 执行get请求
        CloseableHttpClient closeableHttpClient = HttpClientBuilder.create().build();
        HttpResponse httpResponse = closeableHttpClient.execute(httpGet);
        int statusCode = httpResponse.getStatusLine().getStatusCode();
        HttpEntity entity = httpResponse.getEntity();
        if (entity != null) {
            content = EntityUtils.toString(entity);
        }

        if (statusCode != HttpStatus.SC_OK) {
            content = "";
            log.error("返回结果：" + content);
        }
        httpGet.abort();
        return content;
    }

    /**
     * 火空同城异站用get请求
     *
     * @param url
     * @return
     * @throws IOException
     */
    public static String get(String url) throws IOException {
        String content = "";
        HttpGet httpGet = new HttpGet(url);
        // 执行get请求
        CloseableHttpClient closeableHttpClient = HttpClientBuilder.create().build();
        HttpResponse httpResponse = closeableHttpClient.execute(httpGet);
        int statusCode = httpResponse.getStatusLine().getStatusCode();
        HttpEntity entity = httpResponse.getEntity();
        if (entity != null) {
            content = EntityUtils.toString(entity);
        }
        if (statusCode != HttpStatus.SC_OK) {
            log.error("返回结果：" + content);
            content = "";
        }
        return content;
    }

    public static String doGet(String url, CloseableHttpClient closeableHttpClient) throws IOException {
        ResponseHandler<String> handler = new BasicResponseHandler();
        HttpGet request = new HttpGet(url);
        String response = null;
        try {
            response = closeableHttpClient.execute(request, handler);
        } catch (ConnectionClosedException e) {
            if (closeableHttpClient == null) {
                HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
                closeableHttpClient = httpClientBuilder.build();
            }
            response = closeableHttpClient.execute(request, handler);
        }
        request.abort();
        return response;
    }

    public static String doPost(String url, String json, CloseableHttpClient closeableHttpClient) throws IOException {
        String content = null;
        HttpPost post = new HttpPost(url);
        StringEntity postingString = new StringEntity(json, "utf-8");// json传递
        postingString.setContentType("application/json;charset=utf-8");
        post.setEntity(postingString);
        HttpResponse response = closeableHttpClient.execute(post);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == HttpStatus.SC_OK) {
            content = EntityUtils.toString(response.getEntity());
        } else {
            log.error("返回结果：" + content);
        }
        post.abort();
        if (response != null) {
            EntityUtils.consumeQuietly(response.getEntity());
        }
        return content;
    }

    /**
     * 发送httpPost请求,调用公共班次接口
     *
     * @return
     */
    public static String doPostTrain(String url, String json, HttpClient closeableHttpClient) throws Exception {
        String responseStr = "";
        try {
            HttpPost httpPost = new HttpPost(url);
            List<BasicNameValuePair> nvps = new ArrayList<>();
            nvps.add(new BasicNameValuePair("jsonStr", json));
            httpPost.setEntity(new UrlEncodedFormEntity(nvps));
            HttpResponse response = closeableHttpClient.execute(httpPost);
            httpPost.setEntity(new UrlEncodedFormEntity(nvps, "utf-8"));
            responseStr = EntityUtils.toString(response.getEntity(), "UTF-8");
        } catch (Exception e) {
            log.error(url + ",异常：" + json + e.getMessage());
        }
        return responseStr;
    }

    public static String flightPost(String url, String json, HttpClient httpClient) throws IOException {
        String content = null;
        HttpPost post = new HttpPost(url);
        StringEntity postingString = new StringEntity(json, "utf-8");// json传递
        postingString.setContentType("application/json;charset=utf-8");
        post.setEntity(postingString);
        HttpResponse response = httpClient.execute(post);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == HttpStatus.SC_OK) {
            content = EntityUtils.toString(response.getEntity());
        } else {
            log.error("返回结果：" + content);
        }
        post.abort();
        if (response != null) {
            EntityUtils.consumeQuietly(response.getEntity());
        }
        return content;
    }


    public static String post(String url, String request, CloseableHttpClient closeableHttpClient, Map<String, String> headers, String charset) {
        if (charset == null || charset.trim().isEmpty()) {
            charset = "UTF-8";
        }
        CloseableHttpResponse response = null;
        HttpEntity entity = null;
        String content = "";

        HttpPost httpPost = new HttpPost(url);
        // 设置HTTP头信息
        if (headers != null && !headers.isEmpty()) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                Header header = new BasicHeader(entry.getKey(), entry.getValue());
                httpPost.addHeader(header);
            }
        } else {
            httpPost.addHeader(new BasicHeader("Content-Type", "text/xml"));
            httpPost.addHeader(new BasicHeader("charset", "utf-8"));
        }
        // 设置参数
        StringEntity myEntity = new StringEntity(request, "UTF-8");
        httpPost.setEntity(myEntity);

        try {
            response = closeableHttpClient.execute(httpPost);
            entity = response.getEntity();
            content = EntityUtils.toString(entity, charset);
        } catch (ClientProtocolException e) {
            log.error("返回结果：" + content, e);
        } catch (IOException e) {
            log.error("返回结果：" + content, e);
        } finally {
            if (entity != null) {
                try {
                    EntityUtils.consume(entity);
                } catch (IOException e) {
                    log.error("返回结果：" + content, e);
                }
            }

            if (response != null) {
                try {
                    response.close();

                } catch (IOException e) {
                    log.error("返回结果：" + content, e);
                }
            }
            httpPost.abort();
        }
        return content;
    }

}

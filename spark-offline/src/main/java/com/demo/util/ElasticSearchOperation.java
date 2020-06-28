package com.demo.util;

import com.alibaba.fastjson.JSONObject;
import com.demo.common.ConfigUtil;
import com.demo.conf.CommonConfig;
import com.demo.conf.ESConfig;
import com.demo.demo.EsDataOffline;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ElasticSearchOperation {

    private Logger logger = LoggerFactory.getLogger(ElasticSearchOperation.class);

    /**
     * 初始化client
     */
    public RestHighLevelClient getRestHighLevelClient() {
        if (CommonConfig.isLocalEnvConfig()) {
            return EsDataOffline.getRestHighLevelClient();
        } else {
            return getOnlineRestHighLevelClient();
        }
    }

    public RestHighLevelClient getOnlineRestHighLevelClient() {
        String host = ConfigUtil.getPros("es.url.host");
        Integer port = Integer.valueOf(ConfigUtil.getPros("es.url.port"));
        String pathPrefix = ConfigUtil.getPros("es.url.path.prefix");
        HttpHost httpHost = new HttpHost(host, port, "http");
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(httpHost).setPathPrefix(pathPrefix));
        return client;
    }

    /**
     * 删除es索引
     *
     * @param client
     * @param esIndex
     * @throws IOException
     */
    public void deleteIfExistEsIndex(RestHighLevelClient client, String esIndex) throws IOException {
        GetIndexRequest request = new GetIndexRequest();
        request.indices(esIndex);
        boolean exists = client.indices().exists(request);
        if (exists) {
            logger.info("===============存在索引为==========" + esIndex);
            DeleteIndexRequest requestD = new DeleteIndexRequest(esIndex);
            DeleteIndexResponse deleteIndexResponse = client.indices().delete(requestD);
            logger.info("===============成功删除索引" + esIndex + "==============是否成功:" + deleteIndexResponse.isAcknowledged());
        } else {
            logger.info("===============不存在索引为==========" + esIndex);
        }
    }

    /**
     * 创建 一个es索引
     *
     * @param client
     * @param esIndex 索引名
     * @param esType  索引type
     * @param source  mapping字符串
     * @throws IOException
     */
    public void createEsIndex(RestHighLevelClient client, String esIndex, String esType, String source) throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(esIndex);
        //创建分片数量和副本数量
        createIndexRequest.settings(Settings.builder()
                .put("index.number_of_shards", ConfigUtil.getPros("index.number_of_shards"))
                .put("index.number_of_replicas", ConfigUtil.getPros("index.number_of_replicas"))
        );
        createIndexRequest.mapping(esType, source, XContentType.JSON);
        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest);
        logger.info("建立index:" + esType + "是否成功" + createIndexResponse.isAcknowledged());
    }

    /**
     * 为新的索引添加索引别名关联
     *
     * @param client
     * @param index      索引名
     * @param indexAlias
     * @throws IOException
     */
    public void createIndexAlias(RestHighLevelClient client, String index, String indexAlias) throws IOException {
        logger.info("=========为新的索引" + index + "添加索引别名关联" + indexAlias);
        IndicesAliasesRequest requestAddAlias = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions aliasAction =
                new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD).index(index).alias(indexAlias);
        requestAddAlias.addAliasAction(aliasAction);
        IndicesAliasesResponse indicesAliasesResponse = client.indices().updateAliases(requestAddAlias);
        logger.info("=========为新的索引" + index + "添加索引别名关联 是否成功：" + indicesAliasesResponse.isAcknowledged());
    }

    /**
     * delete别名与索引的关联
     *
     * @param client
     * @param index
     * @param indexAlias
     * @throws IOException
     */
    public void removeAliasMapIndex(RestHighLevelClient client, String index, String indexAlias) throws IOException {
        GetAliasesRequest requestE = new GetAliasesRequest();
        requestE.aliases(indexAlias);
        requestE.indices(index);
        IndicesAliasesRequest requestAddAlias = new IndicesAliasesRequest();
        if (client.indices().existsAlias(requestE)) {
            IndicesAliasesRequest.AliasActions removeAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE).index(index).alias(indexAlias);
            requestAddAlias.addAliasAction(removeAction);
            IndicesAliasesResponse indicesAliasesResponseD = client.indices().updateAliases(requestAddAlias);
            logger.info("==========存在索引别名，删除旧索引" + index + "别名" + indexAlias + "关联是否成功===============" + indicesAliasesResponseD.isAcknowledged());
        }
    }

    /**
     * 解除所有旧的索引别名关联
     *
     * @param client
     * @param index
     * @param indexAlias
     */
    public void deleteIfExistOldIndex(RestHighLevelClient client, String index, String indexAlias) {
        logger.info("==========执行开始：如果旧索引关联别名，则解除===============");
        String endpoint = "/" + indexAlias + "/_alias?format=json";
        try {
            Response response = client.getLowLevelClient().performRequest("GET", endpoint);
            String json = EntityUtils.toString(response.getEntity());
            Map<String, Object> map = JSONObject.parseObject(json, Map.class);
            if (map != null && map.size() > 0) {
                for (String oldIndex : map.keySet()) {
                    if (!oldIndex.equals(index)) {
                        removeAliasMapIndex(client, oldIndex, indexAlias);
                        logger.info("==========删除的旧索引别名关联的索引是===============" + oldIndex);
                    }
                }
            }
            logger.info("==========操作结束===============");
        } catch (IOException e) {
            logger.error("删除索引失败,原因:" + e.getMessage(), e);
        }
    }

    /**
     * 关联新索引与别名,
     * 删除别名下不是新索引的所有旧索引.
     *
     * @param client
     * @param newIndex
     * @param indexAlias
     * @throws IOException
     */
    public void renameIndexAlias(RestHighLevelClient client, String newIndex, String indexAlias) throws IOException {
        //关联newIndex到indexAlias
        createIndexAlias(client, newIndex, indexAlias);
        //删除所旧索引与别名的关联
        deleteIfExistOldIndex(client, newIndex, indexAlias);
    }

    /**
     * Es存储数据流程
     *
     * @param redisData
     * @param client
     * @param middleIndex
     * @param source
     * @param indexType
     */
    public void processEsAll(Dataset<Row> redisData, RestHighLevelClient client, String middleIndex, String source, String indexType) throws IOException {
        processEsAll(redisData, client, middleIndex, source, indexType, new HashMap<>());
    }

    /**
     * Es存储数据流程
     *
     * @param redisData
     * @param client
     * @param middleIndex
     * @param source
     * @param indexType
     * @param paramMap
     * @throws IOException
     */
    public void processEsAll(Dataset<Row> redisData, RestHighLevelClient client, String middleIndex, String source, String indexType, Map<String, String> paramMap) throws IOException {
        try {
            //创建Es索引
            String aliasIndex = ESConfig.PRE_INDEX + "." + middleIndex;
            String indexDate = aliasIndex + "." + DateUtils.nowDate();
            deleteIfExistEsIndex(client, indexDate);
            createEsIndex(client, indexDate, indexType, source);
            //创建完索引后添加数据
            String resource = indexDate + "/" + indexType;
            JavaEsSparkSQL.saveToEs(redisData, resource, paramMap);
            // 为新的索引添加索引别名关联
            createIndexAlias(client, indexDate, aliasIndex);
            //如果旧的索引别名关联存在，删除旧的索引别名关联
            deleteIfExistOldIndex(client, indexDate, aliasIndex);
            logger.info("索引:" + indexDate + "别名:" + aliasIndex + "关联成功");
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                logger.error("关闭Es client失败" + e.getMessage(), e);
            }
        }
    }
}
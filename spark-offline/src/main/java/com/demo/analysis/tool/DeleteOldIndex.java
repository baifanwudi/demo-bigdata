package com.demo.analysis.tool;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.demo.base.AbstractSparkSql;
import com.demo.util.ElasticSearchOperation;
import com.demo.util.DateUtils;
import org.apache.commons.cli.*;
import org.apache.http.util.EntityUtils;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * 业务：删除索引，第一个参数为 业务名，以逗号分隔，
 * 第二个参数是距离当前第几天，一般为负数，这样就会自动删除 业务名+日期的索引,
 * 第三个参数是是否指定索引删除
 *
 * @author allen.bai
 */
public class DeleteOldIndex extends AbstractSparkSql {

       ElasticSearchOperation elasticSearchOperation=new ElasticSearchOperation();

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        CommandLine cmd = parseArgs(args);
        String indexNames = cmd.getOptionValue("names");
        //默认删除7天前数据
        Integer sub = Integer.parseInt(cmd.getOptionValue("date", "-7"));
        Boolean isLike = Boolean.parseBoolean(cmd.getOptionValue("rlike", "true"));
        logger.info("要删除index名为:" + indexNames + ",删除时间为:" + sub + "之前,是否模糊匹配:" + isLike);

        RestHighLevelClient client=elasticSearchOperation.getRestHighLevelClient();
        try {
            if (isLike) {
                String subDate = DateUtils.nowAfterOrBeforeDay(sub);
                logger.info("开始删除与索引'" + indexNames + "'模糊匹配且时间在" + subDate + "之前的索引");
                deleteAllBusinessesIndex(client, indexNames, subDate);
            } else {
                //删除指定索引
                logger.info("开始删除索引：" + indexNames);
                elasticSearchOperation.deleteIfExistEsIndex(client, indexNames);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
    }

    public static void main(String[] args) throws IOException {
        DeleteOldIndex deleteOldIndex = new DeleteOldIndex();
        deleteOldIndex.runAll(args);
    }

    private static CommandLine parseArgs(String[] args) {
        CommandLine commandLine = null;
        try {
            Options options = new Options();
            Option namesOption = new Option("n", "names", true, "要被删除的index名字,按','分割符,示例-n trafficwisdom.route_transfer,trafficwisdom.test");
            namesOption.setRequired(true);
            options.addOption(namesOption);
            Option dateOption = new Option("d", "date", true, "删date之前的index,默认删除7天前的index,示例-d -7");
            options.addOption(dateOption);
            Option rlikeOption = new Option("r", "rlike", true, "判断names是否是模糊匹配,默认是true,如果是false指定删除names的索引名,示例-r true");
            options.addOption(rlikeOption);

            CommandLineParser parser = new PosixParser();
            commandLine = parser.parse(options, args);
            return commandLine;
        } catch (org.apache.commons.cli.ParseException e) {
            e.printStackTrace();
            System.err.println("|-n or --names 表示要删除索引名或者索引的模糊匹配,\n" +
                    "|-d or --date 表示删除date之前的索引,\n" +
                    "|-r or --rlike 表示-n传入是否模糊匹配,false(表示--n传入的是索引名),true(表示--n传入索引的模糊匹配)");
            System.exit(-1);
        }
        return commandLine;
    }

    /**
     * 删除别名，指定天数某天数据
     *
     * @param client
     * @param multiRegex
     * @param subDate
     * @throws IOException
     * @throws ParseException
     */
    private void deleteAllBusinessesIndex(RestHighLevelClient client, String multiRegex, String subDate)
            throws IOException, ParseException {
        String[] allIndex = multiRegex.split(",");
        for (String indexAlias : allIndex) {
            deleteOneBusinessIndex(client, indexAlias, subDate);
        }
    }

    /**
     * 删除一个indexRegex模糊匹配所有index,且时间在subDate之前
     *
     * @param client
     * @param indexRegex
     * @param subDate
     * @throws IOException
     * @throws ParseException
     */
    private void deleteOneBusinessIndex(RestHighLevelClient client, String indexRegex, String subDate)
            throws IOException, ParseException {
        LocalDate subLocalDate=LocalDate.parse(subDate,DateUtils.TIME_FORMAT_YYYY_MM_DD);
        String endpoint = "/_cat/indices/" + indexRegex + ".*?format=json&h=index";
        logger.info("elasticsearch请求链接为" + endpoint);
        Response response = client.getLowLevelClient().performRequest("GET", endpoint);
        String json = EntityUtils.toString(response.getEntity());
        List<Map<String, String>> list = JSON.parseObject(json, new TypeReference<List<Map<String, String>>>() {
        });
        for (Map<String, String> map : list) {
            String oldIndex = map.get("index").trim();
            String[] sb = oldIndex.split("\\.");
            if (sb.length == 3) {
                String dateStr = sb[2];
                LocalDate tmpLocalDate=LocalDate.parse(dateStr,DateUtils.TIME_FORMAT_YYYY_MM_DD);
                if(tmpLocalDate.compareTo(subLocalDate)<1){
                    logger.info("开始删除index:" + oldIndex);
                    elasticSearchOperation.deleteIfExistEsIndex(client, oldIndex);
                }
            }
        }
    }
}

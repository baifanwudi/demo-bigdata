package com.demo.demo;

import com.demo.base.AbstractSparkSql;

import com.demo.common.HDFSFileSystem;
import org.apache.hadoop.fs.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;


public class FileSystemDemo extends AbstractSparkSql {

    FileSystem fileSystem= HDFSFileSystem.fileSystem;
    @Override
    public void executeProgram(String[] args,SparkSession spark) throws IOException {

        String path="/warehouse/TrafficHuixing/ext_basestopoverstationinfo";
        RemoteIterator<LocatedFileStatus> files=fileSystem.listFiles(new Path(path),true);
        while(files.hasNext()){
            System.out.println(files.next().toString());
        }
        Dataset<Row> result=spark.sql("select * from city_station_map limit 100");
        result.show();
    }

    public  void renamePath(String src,String dest) throws IOException {
        if(fileSystem.exists(new Path(dest))){
            System.out.println("=================文件存在=================================");
            fileSystem.delete(new Path(dest),true);
        }
        FileStatus[] fs =  fileSystem.listStatus(new Path(src));
        String parquetPath="";
        for(FileStatus status:fs){
            if(status.getPath().toString().endsWith("parquet")){
                parquetPath=status.getPath().toString();
            }
        }
        fileSystem.rename(new Path(parquetPath),new Path(dest));
        System.out.println("=================修改文件名成功================================");
    }

    public static void main(String[] args) throws IOException {
        FileSystemDemo fileSystemDemo=new FileSystemDemo();
        fileSystemDemo.runAll(args,false);
    }
}

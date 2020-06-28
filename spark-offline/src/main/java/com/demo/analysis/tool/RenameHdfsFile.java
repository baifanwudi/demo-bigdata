package com.demo.analysis.tool;

import com.demo.base.AbstractSparkSql;
import com.demo.common.HDFSFileSystem;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.*;
import org.apache.spark.sql.SparkSession;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RenameHdfsFile extends AbstractSparkSql {

    private FileSystem fileSystem = HDFSFileSystem.fileSystem;

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        CommandLine cmd = parseArgs(args);
        String srcFileName = cmd.getOptionValue("s");
        String targetFileName = cmd.getOptionValue("t");
        String exceptFileName = cmd.getOptionValue("e");
        Boolean isMulti = Boolean.parseBoolean(cmd.getOptionValue("m", "false"));
        if (!isMulti) {
            logger.info("要修改的文件名为:" + srcFileName + ",修改后的文件名为:" + targetFileName);
            renameOneFile(srcFileName, targetFileName);
        } else {
            logger.info("要批量修改的文件文件路径:" + srcFileName + ",不修改的文件名为:" + exceptFileName);
            renameMultiName(srcFileName, exceptFileName);
        }
    }

    private void renameOneFile(String srcFileName, String targetFileName) throws IOException {
        if (!fileSystem.exists(new Path(srcFileName))) {
            throw new FileNotFoundException("源文件名" + srcFileName + "不存在");
        }
        if (fileSystem.exists(new Path(targetFileName))) {
            throw new FileAlreadyExistsException("目标文件名" + srcFileName + "已经存在,请换一个修改名字");
        }
        fileSystem.rename(new Path(srcFileName), new Path(targetFileName));
        logger.info(srcFileName + "修改为" + targetFileName + "成功!");
    }

    private void renameMultiName(String filePath, String exceptFileName) throws IOException {
        if (!fileSystem.exists(new Path(filePath))) {
            throw new PathNotFoundException("源文件名" + filePath + "不存在");
        }
        FileStatus[] fs = fileSystem.listStatus(new Path(filePath));
        System.out.println("-----------------------------------------");
        List<String> exceptFiles= Arrays.<String>asList(exceptFileName.split(","));
        for (FileStatus status : fs) {
            Path originPath = status.getPath();
            String originFileName = originPath.getName();
            if (originFileName.startsWith(".")) {
                String parentPath = originPath.getParent().toString();
                System.out.println("origin-name:"+originFileName+",exceptFileName"+exceptFileName);
                if(exceptFiles.contains(originFileName)){
                    continue;
                }
                String targetFile = parentPath +"/"+originFileName.substring(1);
                logger.info("原始文件名为:" + originPath + ",要修改为文件名:" + targetFile);
                fileSystem.rename(originPath, new Path(targetFile));
            }
        }
        System.out.println("------------------------------------");
    }

    private static CommandLine parseArgs(String[] args) {
        CommandLine commandLine = null;
        try {
            Options options = new Options();
            Option srcFileName = new Option("s", "source-file", true, "源文件名(包含前缀)");
            srcFileName.setRequired(true);
            options.addOption(srcFileName);
            OptionGroup optionGroup = new OptionGroup();
            optionGroup.addOption(new Option("t", "target-file", true, "要修改的文件名,包含文件全路径"));
            optionGroup.addOption(new Option("e", "except-file", true, "批量修改的,不需要修改的文件名(不含路径),','分割"));
            optionGroup.setRequired(true);
            options.addOptionGroup(optionGroup);
            Option isMultiRename = new Option("m", "is-multi", true, "是否批量修改");

            options.addOption(isMultiRename);
            CommandLineParser parser = new PosixParser();
            commandLine = parser.parse(options, args);
            return commandLine;
        } catch (org.apache.commons.cli.ParseException e) {
            e.printStackTrace();
            System.err.println("|-s or --source-file 源文件名,\n" +
                    "|-t or --target-file 要修改的文件名 " +
                    "|-e or --except-file 批量修改中,不需要修改文件名(不包含路径)" +
                    "|-m or --is-multi 是否批量修改");
            System.exit(-1);
        }
        return commandLine;
    }

    public static void main(String[] args) throws IOException {
        RenameHdfsFile fileRename = new RenameHdfsFile();
        fileRename.runAll(args);
    }
}

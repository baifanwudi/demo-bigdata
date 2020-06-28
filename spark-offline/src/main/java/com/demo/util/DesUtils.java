package com.demo.util;


import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class DesUtils {

    public static String encoderByMd5(String str) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        //确定计算方法
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        Base64.Encoder encoder = Base64.getEncoder();
        //加密后的字符串
        String encodeStr = encoder.encodeToString(md5.digest(str.getBytes("utf-8")));
        return encodeStr;
    }
}

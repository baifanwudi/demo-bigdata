/**
 * Copyright (c) 2005-2012 springside.org.cn
 */
package com.demo.util;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.IvParameterSpec;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;

/**
 * 封装各种格式的编码解码工具类. 1.Commons-Codec的 hex/base64 编码 2.自制的base62 编码
 * 3.Commons-Lang的xml/html escape 4.JDK提供的URLEncoder
 *
 * @author calvin
 * @version 2013-01-15
 */
public class EncodeUtils {

    private static final String DEFAULT_URL_ENCODING = "UTF-8";
    private final static char[] hexArray = "0123456789ABCDEF".toCharArray(); // Hex进制转换常量
    private static final char[] hexDigits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    private static final byte[] desIv = {0x12, 0x34, 0x56, 0x78, (byte) 0x90, (byte) 0xab, (byte) 0xcd, (byte) 0xef};// des
    // 偏移量
    private static String DES_SECRET = "L82V6ZVD6J"; // 默认密钥
    private static BitSet dontNeedEncoding;
    static {
        dontNeedEncoding = new BitSet(256);
        int i;
        for (i = 'a'; i <= 'z'; i++) {
            dontNeedEncoding.set(i);
        }
        for (i = 'A'; i <= 'Z'; i++) {
            dontNeedEncoding.set(i);
        }
        for (i = '0'; i <= '9'; i++) {
            dontNeedEncoding.set(i);
        }
        dontNeedEncoding.set(' '); /* encoding a space to a + is done
         * in the encode() method */
        dontNeedEncoding.set('-');
        dontNeedEncoding.set('_');
        dontNeedEncoding.set('.');
        dontNeedEncoding.set('*');
    }

    /**
     * Base64编码.
     */
    public static String encodeBase64(byte[] input) {
        return new String(Base64.encodeBase64(input));
    }

    /**
     * Base64编码.
     */
    public static String encodeBase64(String input) {
        try {
            return new String(Base64.encodeBase64(input.getBytes(DEFAULT_URL_ENCODING)));
        } catch (UnsupportedEncodingException e) {
            return "";
        }
    }
    /**
     * Base64解码.
     */
    public static byte[] decodeBase64(String input) {
        return Base64.decodeBase64(input.getBytes());
    }
    /**
     * Base64解码.
     */
    public static String decodeBase64String(String input) {
        try {
            return new String(Base64.decodeBase64(input.getBytes()), DEFAULT_URL_ENCODING);
        } catch (UnsupportedEncodingException e) {
            return "";
        }
    }

    /**
     * MD5加密
     *
     * @param plainText
     * @return
     */
    public static String md5(String plainText) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(plainText.getBytes());
            byte b[] = md.digest();
            int i;
            StringBuffer buf = new StringBuffer("");
            for (int offset = 0; offset < b.length; offset++) {
                i = b[offset];
                if (i < 0) {
                    i += 256;
                }
                if (i < 16) {
                    buf.append("0");
                }
                buf.append(Integer.toHexString(i));
            }
            // 32位加密
            return buf.toString();
            // 16位的加密
            // return buf.toString().substring(8, 24);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }
    /**
     * 二进制转16进制
     *
     * @param bytes
     * @return
     */
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    /**
     * MD5 hash，同C# ComputeHash
     *
     * @param str
     * @return
     */
    public static byte[] md5Hash(String str) {
        return md5Hash(str, "UTF-8");
    }

    /**
     * MD5 hash，同C# ComputeHash
     *
     * @param str
     * @param charset
     * @return
     */
    public static byte[] md5Hash(String str, String charset) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(str.getBytes(charset));
            byte[] result = md.digest();
            return result;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 将每个字节与0xFF进行与运算，然后转化为10进制，然后借助于Integer再转化为16进制
     *
     * @param input
     * @return
     */
    public static String getMD5(String input) {
        byte[] bytes = md5Hash(input);
        return bytesToHex(bytes);
    }

    /**
     * DES使用默认公钥加密字符串
     *
     * @param encryptStr 需加密的字符串
     * @return 加密后的字符串
     */
    public static String desEncrypt(String encryptStr) {
        return desEncrypt(encryptStr, DES_SECRET);
    }

    /**
     * DES加密字符串,BASE64
     *
     * @param encryptStr 需加密的字符串
     * @param key        密钥
     * @return 加密后的字符串
     */
    public static String desEncrypt(String encryptStr, String key) {
        try {
            return EncodeUtils.encodeBase64(desEncrypt(encryptStr.getBytes(), key));
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * DES加密字节数组
     *
     * @param arrB 需加密的字节数组
     * @param key  密钥
     * @return 加密后的字节数组
     * @throws Exception
     */
    public static byte[] desEncrypt(byte[] arrB, String key) throws Exception {
        DESKeySpec desKeySpec = new DESKeySpec(key.getBytes());
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
        SecretKey secretKey = keyFactory.generateSecret(desKeySpec);
        IvParameterSpec ivp = new IvParameterSpec(EncodeUtils.desIv);
        Cipher encryptCipher = Cipher.getInstance("DES/CBC/PKCS5Padding");
        encryptCipher.init(Cipher.ENCRYPT_MODE, secretKey, ivp);
        return encryptCipher.doFinal(arrB);
    }

    /**
     * DES解密字节数组
     *
     * @param arrB 需解密的字节数组
     * @param key  密钥
     * @return 解密后的字节数组
     * @throws Exception
     */
    public static byte[] desDecrypt(byte[] arrB, String key) throws Exception {
        DESKeySpec desKeySpec = new DESKeySpec(key.getBytes());

        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
        SecretKey secretKey = keyFactory.generateSecret(desKeySpec);
        IvParameterSpec ivp = new IvParameterSpec(EncodeUtils.desIv);

        Cipher decryptCipher = Cipher.getInstance("DES/CBC/PKCS5Padding");
        decryptCipher.init(Cipher.DECRYPT_MODE, secretKey, ivp);

        return decryptCipher.doFinal(arrB);
    }

    public static String MD5_32Bit(String s) {
        String result = null;
        if (StringUtils.isEmpty(s)) {
            return result;
        }
        try {
            byte[] btInput = s.getBytes("utf-8");
            // 获得MD5摘要算法的 MessageDigest 对象
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            // 使用指定的字节更新摘要
            messageDigest.update(btInput);
            // 获得密文
            byte[] md = messageDigest.digest();
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            char str[] = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            result = new String(str).toLowerCase();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}

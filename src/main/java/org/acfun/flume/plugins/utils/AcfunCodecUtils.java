package org.acfun.flume.plugins.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.IvParameterSpec;
/**
 * 
 * 编码加解密相关工具类
 * 
 * @author user
 *
 */
public class AcfunCodecUtils {

	
	
	/**
	   * DES解密
	   * 
	   * @param message 要被解密字节数组
	   * @param key	
	   * @return
	   * @throws Exception
	   */
	  public static byte[] decrypt(byte[] message,String key) throws Exception{
	        Cipher cipher = Cipher.getInstance("DES/CBC/PKCS5Padding");
	        DESKeySpec desKeySpec = new DESKeySpec(key.getBytes("UTF-8"));
	        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
	        SecretKey secretKey = keyFactory.generateSecret(desKeySpec);
	        IvParameterSpec iv = new IvParameterSpec(key.getBytes("UTF-8"));
	        cipher.init(Cipher.DECRYPT_MODE, secretKey,iv);
	        return cipher.doFinal(message);
	  }
	  /**
	   * DES加密
	   * 
	   * @param message 要被加密字节数组
	   * @param key
	   * @return
	   * @throws Exception
	   */
	  public static byte[] encrypt(byte[] message,String key) throws Exception {
	        Cipher cipher = Cipher.getInstance("DES/CBC/PKCS5Padding");
	        DESKeySpec desKeySpec = new DESKeySpec(key.getBytes("UTF-8"));
	        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
	        SecretKey secretKey = keyFactory.generateSecret(desKeySpec);
	        IvParameterSpec iv = new IvParameterSpec(key.getBytes("UTF-8"));
	        cipher.init(Cipher.ENCRYPT_MODE, secretKey,iv);
	        return cipher.doFinal(message);
	  }
	  
	  /**
	   * 先Gzip压缩再Des加密
	   * 
	   * @param str
	   * @param key DES秘钥
	   * @return
	   * @throws UnsupportedEncodingException 
	   */
	  public static String compressGzipAndDesEncryption(String str,String key) throws UnsupportedEncodingException {
	        ByteArrayInputStream bais = new ByteArrayInputStream(str.getBytes("UTF-8"));
	        ByteArrayOutputStream baos = new ByteArrayOutputStream();

	        String  result = null;
	        // gzip
	        GZIPOutputStream gos;
	        try {
	            gos = new GZIPOutputStream(baos);
	            int count;
	            byte data[] = new byte[1024];
	            while ((count = bais.read(data, 0, 1024)) != -1) {
	                gos.write(data, 0, count);
	            }
	            gos.finish();
	            gos.close();

	            byte[] output = baos.toByteArray();
	            baos.flush();
	            baos.close();
	            bais.close();

	            result = parseByte2HexStr(encrypt(output,key));

	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	        return result;
	    }
	  /**
	   * 先Des解密再Gzip解压
	   * @param str
	   * @param key DES秘钥
	   * @return
	   * @throws Exception
	   */
	  public static String desDecryptionAndUncompressGzip(String str,String key) throws Exception {
		   byte[] decrypt = decrypt(parseHexStr2Byte(str),key);
		   ByteArrayOutputStream out = new ByteArrayOutputStream();   
		   ByteArrayInputStream in = new ByteArrayInputStream(decrypt);
		   GZIPInputStream gunzip = new GZIPInputStream(in);   
		   byte[] data = new byte[1024];   
		   int count;   
		   while ((count = gunzip.read(data,0,1024))!=-1) {   
		    out.write(data, 0, count);   
		   }
		   return out.toString();   
	  }
	  
	  public static byte[] parseHexStr2Byte(String hexStr) {
			if (hexStr.length() < 1)
				return null;
			byte[] result = new byte[hexStr.length() / 2];
			for (int i = 0; i < hexStr.length() / 2; i++) {
				int high = Integer.parseInt(hexStr.substring(i * 2, i * 2 + 1), 16);
				int low = Integer.parseInt(hexStr.substring(i * 2 + 1, i * 2 + 2),
						16);
				result[i] = (byte) (high * 16 + low);
			}
			return result;
		}
	  
	  
	  public static String parseByte2HexStr(byte buf[]) {
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < buf.length; i++) {
				String hex = Integer.toHexString(buf[i] & 0xFF);
				if (hex.length() == 1) {
					hex = '0' + hex;
				}
				sb.append(hex.toUpperCase());
			}
			return sb.toString();
		}
}

package org.acfun.flume.plugins.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class AcfunFileUtils {

	public static String getStringFromFilePath(String absolutePath) throws IOException{
		StringBuffer sb = new StringBuffer();
		try {
			File file = new File(absolutePath);
			if (file.isFile() && file.exists()) { // 判断文件是否存在
				InputStreamReader read = new InputStreamReader(new FileInputStream(file));
				BufferedReader bufferedReader = new BufferedReader(read);
				String lineTxt = null;
				while ((lineTxt = bufferedReader.readLine()) != null) {
					sb.append(lineTxt);
				}
				read.close();
			} else {
				throw new FileNotFoundException("找不到指定的文件");
			}
		} catch (IOException e) {
			throw new IOException("读取文件内容出错");
		}
		return sb.toString();
	}
	
	public static String getStringFromFile(File file) throws IOException{
		StringBuffer sb = new StringBuffer();
		try {
			if (file.isFile() && file.exists()) { // 判断文件是否存在
				InputStreamReader read = new InputStreamReader(new FileInputStream(file));
				BufferedReader bufferedReader = new BufferedReader(read);
				String lineTxt = null;
				while ((lineTxt = bufferedReader.readLine()) != null) {
					sb.append(lineTxt);
				}
				read.close();
			} else {
				throw new FileNotFoundException("找不到指定的文件");
			}
		} catch (IOException e) {
			throw new IOException("读取文件内容出错");
		}
		return sb.toString();
	}
}

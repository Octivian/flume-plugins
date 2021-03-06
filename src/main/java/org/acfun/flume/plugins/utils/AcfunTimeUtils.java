package org.acfun.flume.plugins.utils;

import org.joda.time.DateTime;

/**
 * 时间处理工具类 统一采用joda time
 * @author user
 *
 */

public class AcfunTimeUtils {
	
	/**
	 * 毫秒时间戳转标准时间戳
	 * @param millisecond
	 * @return 
	 */

	public static String getTimeStampFromMillisecond(long millisecond){
		if(millisecond<10000000000l&&999999999l<millisecond)
			millisecond*=1000;
		DateTime dateTime = new DateTime(millisecond);
		
		return dateTime.toString("yyyy-MM-dd HH:mm:ss");
	}
	
	public static void main(String[] args) {
		System.out.println(getTimeStampFromMillisecond(1464938126l));
	}
	
	
}

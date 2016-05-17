package org.acfun.flume.plugins.utils;

import javax.servlet.http.HttpServletRequest;

public class NetUtils {

	public static String getRealIp(HttpServletRequest request){
		String realIp = request.getHeader("X-Real-IP");
		return realIp;
	}
}

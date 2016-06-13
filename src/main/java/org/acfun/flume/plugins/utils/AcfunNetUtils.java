package org.acfun.flume.plugins.utils;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;

public class AcfunNetUtils {

	public static String getRealIp(HttpServletRequest request){
		if(!StringUtils.isEmpty(request.getHeader("X-Forwarded-For"))){
			return request.getHeader("X-Forwarded-For");
		}else{
			return request.getHeader("X-Real-IP");
		}
	}
}

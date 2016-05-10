package org.acfun.flume.plugins.maidian.httpsource.handler;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.acfun.flume.plugins.utils.AcfunCodecUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

public  class AcfunHttpSourceMaidianHandler implements HTTPSourceHandler {

	protected static final String GET = "GET", POST = "POST", H5 = "h5", APP = "APP", WEB = "WEB";

	private final static Gson gson = new GsonBuilder().disableHtmlEscaping().create();;

	
	protected static final Logger LOG = LoggerFactory.getLogger(AcfunHttpSourceMaidianHandler.class);
	
	protected final static Type listType = new TypeToken<List<Object>>() {}.getType();
	

	public  List<Event> getEvents(HttpServletRequest request) throws Exception{
		String httpType = getHttpType(request);
		if(httpType.equals(APP)){
			return this.handleAppEvents(request);
		}else if(httpType.equals(WEB)){
			
		}else if(httpType.equals(H5)){
			
		}
		return null;
	}
	
	/**
	 * 处理APP上报JSON方法
	 * @param request
	 * @return
	 * @throws IOException 
	 * @throws Exception
	 */
	private  List<Event> handleAppEvents(HttpServletRequest request) throws Exception{
		StringBuffer jb = new StringBuffer();
		String line = null;
		BufferedReader reader = request.getReader();
		while ((line = reader.readLine()) != null)
			jb.append(line);
		LOG.info("APP上报的加密字符串："+jb.toString());
		String json = "";
		try {
			json = AcfunCodecUtils.desDecryptionAndUncompressGzip(jb.toString(), "Acfun!@#");
		} catch (Exception e1) {
			LOG.error("APP解密解压失败，上报数据为："+jb.toString());
			throw new Exception("APP解密解压失败，上报数据为："+jb.toString());
		}
		LOG.info("APP解析后的JSON："+json);
		
		return this.convertAppJsonListToEvents(json);
	}
	
	/**
	 * json字符串转成flume event并设置event的header
	 * 
	 * @param json
	 * @return
	 */
	private  List<Event> convertAppJsonListToEvents(String json){
		
		List<Object> jsonList = gson.fromJson(json, listType);
		
		List<Event> events = new ArrayList<Event>(jsonList.size());
		for (Object e : jsonList) {
			events.add(this.buildAppJsonEvent(e.toString()));
		}
		return events;
	}
	
	/**
	 * 构建event 添加header(timestamp,业务类型)
	 * @param eventString
	 * @return
	 */
	private  Event buildAppJsonEvent(String eventString){
		String eventTimeStamp = StringUtils.substringBetween(eventString, "\"time\":\"", "\"");
		HashMap<String, String> headerMap = new HashMap<String, String>();
		headerMap.put("timestamp", eventTimeStamp);
		headerMap.put("biztype", APP);
		return EventBuilder.withBody(eventString.getBytes(),headerMap);
	}
	

	public void configure(Context context) {}


	/**
	 * 获取http请求业务类型
	 * 
	 * @param request
	 * @return WEB,H5,APP
	 */
	private String getHttpType(HttpServletRequest request) {
		String method = request.getMethod();
		if (method.equals(GET)) {
			if (request.getContextPath().equals(H5)) {
				return H5;
			} else {
				return WEB;
			}
		} else {
			return APP;
		}
	}

	public static void main(String[] args) throws Exception {
		String msg = "[{\"device_id\":\"abc123\",\"uid\":\"aaa\"},{\"device_id\":\"bbb123\",\"uid\":\"bbb\"}]";
		String json = "27226135 cf26dc3f 6b7e81a3 0896fb65 fcad5006 2b5d1436 25791fe2 ddfbc484 e528d93d 2d9a3723 5e5115a8 12f6d2cd bd8fb8f1 346ce138 7e2c0474 901e9594 26d1fc95 eb45610a 40b16130 b91a597b a7bbd08e 7c620950 9a210d3d 24eb3694 fadec7a1 e3f7c75e 81718126 3cdf6a85 4d049b40 3eded193 2989403b 2595f9a7 d87746dc 101c2c04 a7ee8345 bfe76c47 071b4ae6 f30a8eab b2d0847d 31b7ca4b 6d65a415 d6bf29e3 ab70e9f7 caa6934c 8ed04c60 42326f72 cd3e8d89 3a983314 128506fe a0232f8a 2352df8b cb62e2f1 bbb376d9 c87be239";
		json.replace(" ", "");
		String json1 = "[{\"bury_version\" : \"1\",\"is_new_session\" : -1,\"time\":\"123444\"},{\"bury_version\" : \"2333333\",\"is_new_session\" : -1,\"time\":\"00000\"}]";
		Event withBody = EventBuilder.withBody(json1.getBytes("UTF-8"));
//		System.out.println(new String(withBody.getBody()));
		
		
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		List<Object> list = gson.fromJson(json1, listType);
		System.out.println(new String(list.get(0).toString()));
	}
}

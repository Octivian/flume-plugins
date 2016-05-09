package org.acfun.flume.plugins.httpsource.handler.maidian;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.acfun.flume.plugins.utils.CodecUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.reflect.TypeToken;

public abstract class AcfunHttpSourceMaidianHandler implements HTTPSourceHandler {

	protected static final String GET = "GET", POST = "POST", H5 = "h5", APP = "APP", WEB = "WEB";

	protected static final Logger LOG = LoggerFactory.getLogger(AcfunHttpSourceMaidianHandler.class);
	protected final Type listType = new TypeToken<List<JSONEvent>>() {
	}.getType();

	public  List<Event> getEvents(HttpServletRequest request) throws Exception{
		String httpType = getHttpType(request);
		if(httpType.equals(APP)){
			return new AcfunHttpSouceAppHandler().handleEvents(request);
		}else if(httpType.equals(WEB)){
			
		}else if(httpType.equals(H5)){
			
		}
		return null;
	}
	
	public abstract List<Event> handleEvents(HttpServletRequest request) throws Exception;

	public void configure(Context context) {
	}

	protected List<Event> getSimpleEvents(List<Event> events) {
		List<Event> newEvents = new ArrayList<Event>(events.size());
		for (Event e : events) {
			newEvents.add(EventBuilder.withBody(e.getBody(), e.getHeaders()));
		}
		return newEvents;
	}

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
		String msg = "[{\"headers\":{\"h1\":\"v1\",\"h2\":\"v2\"},\"body\":\"hello bodyyyyy\"}]";
		System.out.println(
				new String(CodecUtils.decrypt(CodecUtils.encrypt(msg.getBytes("UTF-8"), "Acfun!@#"), "Acfun!@#")));
		System.out.println(CodecUtils
				.desDecryptionAndUncompressGzip(CodecUtils.compressGzipAndDesEncryption(msg, "Acfun!@#"), "Acfun!@#"));
	}
}

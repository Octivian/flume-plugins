package org.acfun.flume.plugins.maidian.source.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.acfun.flume.plugins.maidian.constant.AcfunMaidianConstants;
import org.acfun.flume.plugins.utils.NetUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class AcfunHttpSouceH5Handler implements HTTPSourceHandler {
	
	
	protected static final Logger LOG = LoggerFactory.getLogger(AcfunHttpSouceH5Handler.class);
	
	private final static Gson gson = new GsonBuilder().disableHtmlEscaping().create();
	
	private static final String[] commonFields = { "bury_version", "device_id", "uid", "event_id", "session_id",
			"time", "previous_page", "network", "refer" };

	private static final Map<String, String[]> detailFieldsMap = new HashMap<String, String[]>();
	static {
		detailFieldsMap.put("200001", new String[] {});
		detailFieldsMap.put("100101", new String[] { "product_id", "device_type", "device_os", "resolution",
				"cooper_id", "browser_version" });
		detailFieldsMap.put("200002", new String[] { "channel_id"});
		detailFieldsMap.put("200003", new String[] { "content_id"});
		detailFieldsMap.put("101010", new String[] { "content_id", "video_id"});
		detailFieldsMap.put("101011", new String[] { "content_id", "video_id"});
		detailFieldsMap.put("400001", new String[] { "content_id", "video_id"});
		detailFieldsMap.put("100001", new String[] { "content_id"});
		detailFieldsMap.put("100002", new String[] { "content_id"});
		detailFieldsMap.put("100004", new String[] { "content_id", "down_entry"});
		detailFieldsMap.put("100006", new String[] { "have_result", "search_content" });
	}

	public void configure(Context context) {}

	public List<Event> getEvents(HttpServletRequest request) throws HTTPBadRequestException, Exception {
		
		List<Event> arrayList = new ArrayList<Event>(1);

		String webLogString = request.getParameter("value");

		String realIpAddress = NetUtils.getRealIp(request);

		LOG.info("H5端获取的数据" + webLogString);
		String[] fields = webLogString.split(",",-1);
		

		if (fields.length < commonFields.length) {
			throw new Exception("缺失公共参数");
		}
		
		String eventId = fields[3];

		StringBuffer sb = new StringBuffer();
		sb.append(realIpAddress + "\t");

		//设置公共字段
		for (int i = 0; i < commonFields.length; i++) {
			sb.append(fields[i] + "\t");
			LOG.info(commonFields[i] + ":" + fields[i]);
		}
		
		
		String[] detailFields = detailFieldsMap.get(eventId);

		if (detailFields == null) {
			throw new Exception("事件ID：" + eventId + "不正确，请参考上报文档");
		}

		if (fields.length != commonFields.length + detailFields.length) {
			throw new Exception("参数个数不匹配，请检查参数");
		}
		
		HashMap<String, String> headerMap = new HashMap<String, String>();
		
		headerMap.put(AcfunMaidianConstants.BIZTYPE, AcfunMaidianConstants.H5);
		
		//根据是否是sessionlog对个性化字段做处理
		if (eventId.equals(AcfunMaidianConstants.APP_JSONV_SESSION_EVENT_ID)) {
			
			headerMap.put(AcfunMaidianConstants.LOGTYPE,AcfunMaidianConstants.SESSIONLOG);
			
			for (int i = 0; i < detailFields.length; i++) {
				sb.append(fields[commonFields.length  + i] + "\t");
				LOG.info(detailFields[i] + ":" + fields[commonFields.length + i]);
			}
			arrayList.add(EventBuilder.withBody(StringUtils.substringBeforeLast(sb.toString(), "\t").getBytes("UTF-8"), headerMap));
			
		}else{
			
			headerMap.put(AcfunMaidianConstants.LOGTYPE,AcfunMaidianConstants.EVENTLOG);
			
			Map<String, String> detailMap = new HashMap<String, String>();

			for (int i = 0; i < detailFields.length; i++) {
				detailMap.put(detailFields[i], fields[commonFields.length  + i]);
				LOG.info(detailFields[i] + ":" + fields[commonFields.length  + i]);
			}
			sb.append(gson.toJson(detailMap));
			
			arrayList.add(EventBuilder.withBody(sb.toString().getBytes("UTF-8"), headerMap));
		}

		return arrayList;
	}

}

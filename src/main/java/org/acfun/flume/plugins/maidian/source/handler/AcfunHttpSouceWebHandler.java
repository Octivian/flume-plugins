package org.acfun.flume.plugins.maidian.source.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.acfun.flume.plugins.maidian.constant.AcfunMaidianConstants;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class AcfunHttpSouceWebHandler implements HTTPSourceHandler {

	protected static final Logger LOG = LoggerFactory.getLogger(AcfunHttpSouceWebHandler.class);
	
	private final static Gson gson = new GsonBuilder().disableHtmlEscaping().create();

	private static final String[] commonFields = { "bury_version", "device_id", "uid", "event_id", "session_id",
			"time", "previous_page", "network", "refer" };

	private static final Map<String, String[]> detailFieldsMap = new HashMap<String, String[]>();
	static {
		detailFieldsMap.put("200001", new String[] {});
		detailFieldsMap.put("100101", new String[] { "product_id", "device_type", "device_os", "resolution",
				"cooper_id", "browser_version" });
		detailFieldsMap.put("200002", new String[] { "channel_id", "child_channel_id"});
		detailFieldsMap.put("200003", new String[] { "content_id"});
		detailFieldsMap.put("200005", new String[] { "channel_id"});
		detailFieldsMap.put("100005", new String[] { "block_type", "block_id", "module_type","module_id","content_type","content_id" });
		detailFieldsMap.put("101010", new String[] { "content_id", "video_id"});
		detailFieldsMap.put("101011", new String[] { "content_id", "video_id"});
		detailFieldsMap.put("400001", new String[] { "content_id", "video_id"});
		detailFieldsMap.put("100007", new String[] { "content_id"});
		detailFieldsMap.put("100008", new String[] { "content_id"});
		detailFieldsMap.put("100009", new String[] { "content_id", "share_to"});
		detailFieldsMap.put("100011", new String[] { "content_id", "is_success"});
		detailFieldsMap.put("100012", new String[] { "content_id", "throw_count", "is_success"});
		detailFieldsMap.put("100015", new String[] { "content_id", "video_id", "danmu_type","danmu_color","danmu_size","is_success"});
		detailFieldsMap.put("100006", new String[] { "have_result", "search_content" });
		detailFieldsMap.put("100016", new String[] { "content_id", "video_id"});
		detailFieldsMap.put("100017", new String[] { "from_content_id", "from_video_id", "content_id" });
	}

	public void configure(Context context) {
	}

	public List<Event> getEvents(HttpServletRequest request) throws HTTPBadRequestException, Exception {

		String webLogString = request.getParameter("value");

		String realIpAddress = request.getHeader("X-Real-IP");
		
		LOG.info("WEB端获取的数据" + webLogString);
		String[] fields = webLogString.split(",");
		
		StringBuffer sb = new StringBuffer();
		sb.append(realIpAddress+"\t");
		
		LOG.info("ip:"+realIpAddress);
		
		for (int i = 0;i<commonFields.length;i++) {
			sb.append(fields[i]+"\t");
			LOG.info(commonFields[i]+":"+fields[i]);
		}
		String eventId = fields[3];
		String[] detailFields = detailFieldsMap.get(eventId);
		
		Map<String,String> detailMap = new HashMap<String,String>();
		
		for (int i = 0; i < detailFields.length; i++) {
			detailMap.put(detailFields[i],fields[commonFields.length-1+i]);
			LOG.info(detailFields[i]+":"+fields[commonFields.length-1+i]);
		}
		sb.append(gson.toJson(detailMap));
		
		HashMap<String, String> headerMap = new HashMap<String, String>();
		headerMap.put(AcfunMaidianConstants.LOGTYPE, eventId.equals(AcfunMaidianConstants.APP_JSONV_SESSION_EVENT_ID)
				? AcfunMaidianConstants.SESSIONLOG : AcfunMaidianConstants.EVENTLOG);
		headerMap.put(AcfunMaidianConstants.BIZTYPE, AcfunMaidianConstants.WEB);
		
		List<Event> arrayList = new ArrayList<Event>();
		
		
		arrayList.add(EventBuilder.withBody(webLogString.getBytes("UTF-8"), headerMap));
		return arrayList;
	}

}

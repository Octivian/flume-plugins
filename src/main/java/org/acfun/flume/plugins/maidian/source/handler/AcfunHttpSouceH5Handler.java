package org.acfun.flume.plugins.maidian.source.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.acfun.flume.plugins.maidian.constant.AcfunMaidianConstants;
import org.acfun.flume.plugins.utils.AcfunNetUtils;
import org.acfun.flume.plugins.utils.AcfunTimeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.joda.time.DateTime;
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
		detailFieldsMap.put("400001", new String[] { "content_id", "video_id" ,"current_time"});
		detailFieldsMap.put("100001", new String[] { "content_id"});
		detailFieldsMap.put("100002", new String[] { "content_id"});
		detailFieldsMap.put("100004", new String[] { "content_id", "down_entry"});
		detailFieldsMap.put("100006", new String[] { "have_result", "search_content" });
	}

	public void configure(Context context) {}

	public List<Event> getEvents(HttpServletRequest request) throws HTTPBadRequestException, Exception {
		
		DateTime now = DateTime.now();
		
		List<Event> arrayList = new ArrayList<Event>(1);

		String webLogString = StringUtils.substringAfter(request.getQueryString(),"value=");
		String realIpAddress = AcfunNetUtils.getRealIp(request);

		LOG.debug("H5端获取的数据" + webLogString);
		String[] fields = webLogString.split(AcfunMaidianConstants.GET_MAIDIAN_LOG_REGEX,-1);
		

		if (fields.length < commonFields.length) {
			throw new Exception("H5端获取的数据" + webLogString+"缺失公共参数");
		}
		
		String eventId = fields[3];

		StringBuffer sb = new StringBuffer();
		sb.append(realIpAddress + "\t");
		

		//设置公共字段
		for (int i = 0; i < commonFields.length; i++) {
			if (i == 5) {
				if(StringUtils.isEmpty(fields[i])){
					sb.append(now.toString("yyyy-MM-dd HH:mm:ss")+"\t");
				}else{
					try {
						sb.append(AcfunTimeUtils.getTimeStampFromMillisecond(Long.valueOf(fields[i])) + "\t");
					} catch (Exception e) {
						throw new Exception("时间戳转换错误，时间戳为：" + fields[i]);
					}
				}
			} else {
				sb.append(fields[i] + "\t");
			}
			LOG.debug(fields[1]+"---"+commonFields[i] + ":" + fields[i]);
		}
		
		
		String[] detailFields = detailFieldsMap.get(eventId);

		if (detailFields == null) {
			throw new Exception("H5端获取的数据" + webLogString+"事件ID：" + eventId + "不正确，请参考上报文档");
		}

		if (fields.length != commonFields.length + detailFields.length) {
			//特殊处理h5心跳错误跳过
			if(!eventId.equals("400001")){
				throw new Exception("H5端获取的数据" + webLogString+"参数个数不匹配，请检查参数");
			}else{
				return null;
			}
		}
		
		HashMap<String, String> headerMap = new HashMap<String, String>();
		
		headerMap.put(AcfunMaidianConstants.BIZTYPE, AcfunMaidianConstants.H5);
		headerMap.put(AcfunMaidianConstants.TIMESTAMP, String.valueOf(now.getMillis()));
		
		//根据是否是sessionlog对个性化字段做处理
		if (eventId.equals(AcfunMaidianConstants.APP_JSONV_SESSION_EVENT_ID)) {
			
			headerMap.put(AcfunMaidianConstants.LOGTYPE,AcfunMaidianConstants.SESSIONLOG);
			
			for (int i = 0; i < detailFields.length; i++) {
				sb.append(fields[commonFields.length  + i] + "\t");
				LOG.debug(fields[1]+"---"+detailFields[i] + ":" + fields[commonFields.length + i]);
			}
			sb.append(now.toString("yyyy-MM-dd HH:mm:ss"));
			arrayList.add(EventBuilder.withBody(sb.toString().getBytes("UTF-8"), headerMap));
			
		}else{
			
			headerMap.put(AcfunMaidianConstants.LOGTYPE,AcfunMaidianConstants.EVENTLOG);
			
			Map<String, String> detailMap = new HashMap<String, String>();

			for (int i = 0; i < detailFields.length; i++) {
				detailMap.put(detailFields[i], fields[commonFields.length  + i]);
				if("current_time".equals(detailFields[i])&&!StringUtils.isNumeric(fields[commonFields.length  + i])){
					LOG.warn("心跳上报时间错误："+fields[1]+"---"+detailFields[i] + ":" + fields[commonFields.length  + i]+"---"+webLogString);
				}
				LOG.debug(fields[1]+"---"+detailFields[i] + ":" + fields[commonFields.length  + i]);
			}
			sb.append(gson.toJson(detailMap)+"\t");
			
			sb.append(now.toString("yyyy-MM-dd HH:mm:ss"));
			
			arrayList.add(EventBuilder.withBody(sb.toString().getBytes("UTF-8"), headerMap));
		}

		return arrayList;
	}

}

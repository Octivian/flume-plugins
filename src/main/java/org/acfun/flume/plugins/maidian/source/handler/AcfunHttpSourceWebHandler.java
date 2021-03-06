package org.acfun.flume.plugins.maidian.source.handler;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.acfun.flume.plugins.maidian.constant.AcfunMaidianConstants;
import org.acfun.flume.plugins.utils.AcfunFileUtils;
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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

public class AcfunHttpSourceWebHandler implements HTTPSourceHandler {

	protected static final Logger LOG = LoggerFactory.getLogger(AcfunHttpSourceWebHandler.class);

	private final static Gson gson = new GsonBuilder().disableHtmlEscaping().create();

	private static  String[] commonFields;
	
	protected final static Type listType = new TypeToken<Map<String, String[]>>() {
	}.getType();
	
	private static final Map<String,Map<String, String[]>> dualDetailFieldsMap = new HashMap<String, Map<String,String[]>>();

//	private static final Map<String, String[]> detailFieldsMap = new HashMap<String, String[]>();
//	static {
//		detailFieldsMap.put("600000", new String[] {});
//		detailFieldsMap.put("200001", new String[] {});
//		detailFieldsMap.put("100101", new String[] { "product_id", "device_type", "device_os", "resolution",
//				"cooper_id", "browser_version" });
//		detailFieldsMap.put("200002", new String[] { "channel_id", "child_channel_id" });
//		detailFieldsMap.put("200003", new String[] { "content_id" });
//		detailFieldsMap.put("200005", new String[] { "channel_id" });
//		detailFieldsMap.put("100005",
//				new String[] { "block_type", "block_id", "module_type", "module_id", "content_type", "content_id" });
//		detailFieldsMap.put("101010", new String[] { "content_id", "video_id" });
//		detailFieldsMap.put("101011", new String[] { "content_id", "video_id" });
//		detailFieldsMap.put("400001", new String[] { "content_id", "video_id", "current_time" });
//		detailFieldsMap.put("100007", new String[] { "content_id" });
//		detailFieldsMap.put("100008", new String[] { "content_id" });
//		detailFieldsMap.put("100009", new String[] { "content_id", "share_to" });
//		detailFieldsMap.put("100011", new String[] { "content_id", "is_success" });
//		detailFieldsMap.put("100012", new String[] { "content_id", "throw_count", "is_success" });
//		detailFieldsMap.put("100015",
//				new String[] { "content_id", "video_id", "danmu_type", "danmu_color", "danmu_size", "is_success" });
//		detailFieldsMap.put("100006", new String[] { "have_result", "search_content" });
//		detailFieldsMap.put("100016", new String[] { "content_id", "video_id" });
//		detailFieldsMap.put("100017", new String[] { "from_content_id", "from_video_id", "content_id" });
//		detailFieldsMap.put("500000", new String[] { "video_type", "video_id", "error_type","error_id"});
//		
//		detailFieldsMap.put("000000", new String[] {});
//	}

	public void configure(Context context) {
		String stringFromFile = "";
		try {
			stringFromFile = AcfunFileUtils.getStringFromFilePath(context.getString(AcfunMaidianConstants.CONFIG_HANDLERS_CONF_PATH));
			LOG.info("new Conf:"+stringFromFile);
			JsonObject o = new JsonParser().parse(stringFromFile).getAsJsonObject();
			commonFields = o.getAsJsonArray("commonFields").toString().replace("[","").replace("]","").replace("\"","").split(",",-1);
			JsonObject detailFields = o.getAsJsonObject("detailFields");
			Set<Entry<String, JsonElement>> entrySet = detailFields.getAsJsonObject().entrySet();
			for (Entry<String, JsonElement> entry : entrySet) {
				Map<String, String[]> detailField = gson.fromJson(entry.getValue(), listType);
				dualDetailFieldsMap.put(entry.getKey(), detailField);
			}
		} catch (IOException e) {
			LOG.error(e.getMessage());
		}
	}

	public List<Event> getEvents(HttpServletRequest request) throws HTTPBadRequestException, Exception {
		
		DateTime now = DateTime.now();

		List<Event> arrayList = new ArrayList<Event>(1);

		String webLogString = StringUtils.substringAfter(request.getQueryString(), "value=");

		String realIpAddress = AcfunNetUtils.getRealIp(request);

		LOG.debug("WEB端获取的数据" + webLogString);
		String[] fields = webLogString.split(AcfunMaidianConstants.GET_MAIDIAN_LOG_REGEX, -1);

		if (fields.length < commonFields.length) {
			throw new Exception("WEB端获取的数据" + webLogString + "缺失公共参数");
		}

		String eventId = fields[3];
		
		String buryVersion = fields[0];

		StringBuffer sb = new StringBuffer();
		sb.append(realIpAddress + "\t");

		// 设置公共字段
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
			LOG.debug(fields[1] + "---" + commonFields[i] + ":" + fields[i]);
		}

		String[] detailFields = dualDetailFieldsMap.get(buryVersion).get(eventId);
		if (detailFields == null) {
			throw new Exception("WEB端获取的数据" + webLogString + "事件ID：" + eventId + "不正确，请参考上报文档");
		}

		if (fields.length != commonFields.length + detailFields.length) {
			throw new Exception("WEB端获取的数据" + webLogString + "参数个数不匹配，请检查参数");
		}

		HashMap<String, String> headerMap = new HashMap<String, String>();

		headerMap.put(AcfunMaidianConstants.BIZTYPE, AcfunMaidianConstants.WEB);
		headerMap.put(AcfunMaidianConstants.TIMESTAMP, String.valueOf(now.getMillis()));

		// 根据是否是sessionlog对个性化字段做处理
		if (eventId.equals(AcfunMaidianConstants.APP_JSONV_SESSION_EVENT_ID)) {

			headerMap.put(AcfunMaidianConstants.LOGTYPE, AcfunMaidianConstants.SESSIONLOG);

			for (int i = 0; i < detailFields.length; i++) {
				sb.append(fields[commonFields.length + i] + "\t");
				LOG.debug(fields[1] + "---" + detailFields[i] + ":" + fields[commonFields.length + i]);
			}
			sb.append(now.toString("yyyy-MM-dd HH:mm:ss"));
			arrayList.add(EventBuilder.withBody(sb.toString().getBytes("UTF-8"),
					headerMap));

		} else {

			headerMap.put(AcfunMaidianConstants.LOGTYPE, AcfunMaidianConstants.EVENTLOG);

			Map<String, String> detailMap = new HashMap<String, String>();

			for (int i = 0; i < detailFields.length; i++) {
				detailMap.put(detailFields[i], fields[commonFields.length + i]);
				LOG.debug(fields[1] + "---" + detailFields[i] + ":" + fields[commonFields.length + i]);
			}
			sb.append(gson.toJson(detailMap)+"\t");
			sb.append(now.toString("yyyy-MM-dd HH:mm:ss"));
			arrayList.add(EventBuilder.withBody(sb.toString().getBytes("UTF-8"), headerMap));
		}

		return arrayList;
	}

}

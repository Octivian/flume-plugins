package org.acfun.flume.plugins.maidian.source.handler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.acfun.flume.plugins.maidian.constant.AcfunMaidianConstants;
import org.acfun.flume.plugins.utils.AcfunCodecUtils;
import org.acfun.flume.plugins.utils.AcfunFileUtils;
import org.acfun.flume.plugins.utils.AcfunNetUtils;
import org.acfun.flume.plugins.utils.AcfunTimeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
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

public class AcfunHttpSourceAppHandler implements HTTPSourceHandler {

	protected static final Logger LOG = LoggerFactory.getLogger(AcfunHttpSourceAppHandler.class);

	protected final static Type listType = new TypeToken<List<Map<String, String>>>() {
	}.getType();
	
	

	private final static Gson gson = new GsonBuilder().disableHtmlEscaping().create();

	protected final static Type detailFieldsType = new TypeToken<Map<String, String[]>>() {
	}.getType();

	private static String[] commonFields;

	private static final Map<String, Map<String, String[]>> dualDetailFieldsMap = new HashMap<String, Map<String, String[]>>();

//	private static final String[] commonFields = { "bury_version", "device_id", "uid", "event_id", "session_id", "time",
//			"previous_page", "network", "refer" };

//	private static final Map<String, Map<String, String[]>> buryVersionMap = new HashMap<String, Map<String, String[]>>();
//
//	private static final Map<String, String[]> detailFieldsMap1 = new HashMap<String, String[]>();
//
//	private static final Map<String, String[]> detailFieldsMap2 = new HashMap<String, String[]>();
//
//	static {
//		detailFieldsMap1.put("100010", new String[] { "launch_type" });
//		detailFieldsMap1.put("100101", new String[] { "product_id", "device_type", "device_brand", "device_model",
//				"device_os", "device_os_version", "resolution", "app_version", "IMEI", "IMSI", "IDFA", "cooper_id" });
//		detailFieldsMap1.put("100100", new String[] { "packages" });
//		detailFieldsMap1.put("110001", new String[] { "content_id" });
//		detailFieldsMap1.put("100015", new String[] { "search_src", "have_result", "search_content" });
//		detailFieldsMap1.put("100013", new String[] { "module_type", "module_id", "content_type", "content_id" });
//		detailFieldsMap1.put("101010", new String[] { "content_id", "video_id", "play_type" });
//		detailFieldsMap1.put("101011", new String[] { "content_id", "video_id" });
//		detailFieldsMap1.put("101012", new String[] { "content_id", "video_id", "play_time" });
//		detailFieldsMap1.put("100030", new String[] { "content_id" });
//		detailFieldsMap1.put("100031", new String[] { "content_id" });
//		detailFieldsMap1.put("100040", new String[] { "content_id", "share_to" });
//		detailFieldsMap1.put("100028", new String[] { "content_id", "enter_src", "is_success" });
//		detailFieldsMap1.put("100041", new String[] { "content_id" });
//		detailFieldsMap1.put("100042", new String[] { "content_id", "throw_count" });
//		detailFieldsMap1.put("100050",
//				new String[] { "content_id", "video_id", "danmu_type", "danmu_color", "danmu_size", "is_success" });
//		detailFieldsMap1.put("110002", new String[] { "channel_id" });
//		detailFieldsMap1.put("110003", new String[] { "child_channel_id" });
//		detailFieldsMap1.put("300001", new String[] { "page_id", "page_action" });
//
//		detailFieldsMap1.put("200004", new String[] {});
//		detailFieldsMap1.put("200005", new String[] {});
//		detailFieldsMap1.put("200006", new String[] {});
//		detailFieldsMap1.put("200007", new String[] {});
//		detailFieldsMap1.put("200035", new String[] {});
//		detailFieldsMap1.put("200025", new String[] {});
//		detailFieldsMap1.put("200023", new String[] {});
//		detailFieldsMap1.put("200020", new String[] {});
//		detailFieldsMap1.put("200018", new String[] {});
//		detailFieldsMap1.put("200026", new String[] {});
//		detailFieldsMap1.put("500001", new String[] {});
//		detailFieldsMap1.put("500002", new String[] {});
//		detailFieldsMap1.put("500003", new String[] {});
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
				Map<String, String[]> detailField = gson.fromJson(entry.getValue(), detailFieldsType);
				dualDetailFieldsMap.put(entry.getKey(), detailField);
			}
		} catch (IOException e) {
			LOG.error(e.getMessage());
		}
	}

	public static void main(String[] args) {

	}

	public List<Event> getEvents(HttpServletRequest request) throws Exception {

		StringBuffer jb = new StringBuffer();
		String line = null;
		String realIpAddress = AcfunNetUtils.getRealIp(request);
		BufferedReader reader = null;
		try {
			reader = request.getReader();
			while ((line = reader.readLine()) != null)
				jb.append(line);
			LOG.debug("APP上报的加密字符串：" + jb.toString());
		} catch (Exception ex) {
			LOG.error("APP上报的加密字符串：" + jb.toString() + "读取BufferedReader失败:" + ex.getMessage());
			throw new Exception("APP上报的加密字符串：" + jb.toString() + "读取BufferedReader失败:" + ex.getMessage());
		}
		String json = "";
		try {
			json = AcfunCodecUtils.desDecryptionAndUncompressGzip(jb.toString(), "Acfun!@#");
		} catch (Exception e) {
			LOG.error("APP解密解压失败，上报数据为：" + jb.toString());
			throw new Exception("APP解密解压失败，上报数据为：" + jb.toString());
		}
		json = StringUtils.trim(json).replace(" ", "").replace("\n", "");
		LOG.info("APP解析后的JSON：" + json);

		List<Map<String, String>> jsonList = gson.fromJson(json, listType);

		return this.convertAppJsonListToEvents(jsonList, realIpAddress);
	}

	/**
	 * json字符串转成flume event并设置event的header
	 * 
	 * @param json
	 * @return
	 * @throws Exception
	 */
	private List<Event> convertAppJsonListToEvents(List<Map<String, String>> jsonList, String realIpAddress)
			throws Exception {

		List<Event> events = new ArrayList<Event>(jsonList.size());
		for (Map<String, String> jsonMap : jsonList) {
			events.add(buildAppJsonEvent(jsonMap, realIpAddress));
		}
		return events;
	}

	/**
	 * 构建event 添加header(业务类型)
	 * 
	 * @param eventString
	 * @return
	 * @throws Exception
	 * @throws UnsupportedEncodingException
	 */
	private Event buildAppJsonEvent(Map<String, String> eventMap, String realIpAddress) throws Exception {

		DateTime now = DateTime.now();

		String buryVersion = eventMap.get(AcfunMaidianConstants.APP_JSONK_BURY_VERSION);
		
		String deviceId = eventMap.get(AcfunMaidianConstants.APP_JSONK_DEVICE_ID);

		String eventId = eventMap.get(AcfunMaidianConstants.APP_JSONK_EVENT_ID);

		HashMap<String, String> headerMap = new HashMap<String, String>();
		headerMap.put(AcfunMaidianConstants.BIZTYPE, AcfunMaidianConstants.APP);
		headerMap.put(AcfunMaidianConstants.TIMESTAMP, String.valueOf(now.getMillis()));
		StringBuffer sb = new StringBuffer();

		sb.append(realIpAddress + "\t");

		LOG.info("解析各个字段为：");
		// 设置公共字段
		for (String string : commonFields) {
			if (string.equals("time")) {
				if (StringUtils.isEmpty(eventMap.get(string))) {
					sb.append(now.toString("yyyy-MM-dd HH:mm:ss") + "\t");
				} else {
					try {
						sb.append(
								AcfunTimeUtils.getTimeStampFromMillisecond(Long.valueOf(eventMap.get(string))) + "\t");
					} catch (Exception e) {
						throw new Exception("时间戳转换错误，时间戳为：" + eventMap.get(string));
					}
				}
			} else {
				sb.append(eventMap.get(string) + "\t");
			}
			
			LOG.info("设备ID为："+deviceId+"——"+string+"："+eventMap.get(string));
		}

		Map<String, String[]> map = dualDetailFieldsMap.get(buryVersion);
		String[] detailFields = map.get(eventId);
		if(map.containsKey(eventId)&&detailFields==null){
			detailFields = new String[]{};
		}
		if(!map.containsKey(eventId)){
			throw new Exception("APP端获取的数据事件ID：" + eventId + ",不正确，请参考上报文档");
		}


		// 根据是否是sessionlog对个性化字段做处理
		if (eventId.equals(AcfunMaidianConstants.APP_JSONV_SESSION_EVENT_ID)) {

			headerMap.put(AcfunMaidianConstants.LOGTYPE, AcfunMaidianConstants.SESSIONLOG);

			for (String string : detailFields) {
				sb.append(eventMap.get(string) + "\t");
				LOG.info("设备ID为："+deviceId+"——"+string+"："+eventMap.get(string));
			}
			sb.append(now.toString("yyyy-MM-dd HH:mm:ss"));
			return EventBuilder.withBody(sb.toString().getBytes(), headerMap);
		} else {

			headerMap.put(AcfunMaidianConstants.LOGTYPE, AcfunMaidianConstants.EVENTLOG);

			Map<String, String> detailMap = new HashMap<String, String>();

			for (String string : detailFields) {
				detailMap.put(string, eventMap.get(string));
				LOG.info("设备ID为："+deviceId+"——"+string+"："+eventMap.get(string));
			}

			sb.append(gson.toJson(detailMap) + "\t");
			sb.append(now.toString("yyyy-MM-dd HH:mm:ss"));
			return EventBuilder.withBody(sb.toString().getBytes(), headerMap);
		}

	}

}

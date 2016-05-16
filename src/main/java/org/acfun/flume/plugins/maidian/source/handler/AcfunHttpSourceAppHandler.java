package org.acfun.flume.plugins.maidian.source.handler;

import java.io.BufferedReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.acfun.flume.plugins.maidian.constant.AcfunMaidianConstants;
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

public class AcfunHttpSourceAppHandler implements HTTPSourceHandler {

	private final static Gson gson = new GsonBuilder().disableHtmlEscaping().create();

	protected static final Logger LOG = LoggerFactory.getLogger(AcfunHttpSourceAppHandler.class);

	protected final static Type listType = new TypeToken<List<Map<String, String>>>() {
	}.getType();

	private static final String[] commonFields = { "bury_version", "device_id", "uid", "event_id", "session_id", "time",
			"previous_page", "network", "refer" };

	private static final Map<String, String[]> detailFieldsMap = new HashMap<String, String[]>();
	static {
		detailFieldsMap.put("100010", new String[] { "launch_type" });
		detailFieldsMap.put("100101", new String[] { "product_id", "device_type", "device_brand", "device_model",
				"device_os", "device_os_version", "resolution", "app_version", "IMEI", "IMSI", "IDFA", "cooper_id" });
		detailFieldsMap.put("100100", new String[] { "packages" });
		detailFieldsMap.put("110001", new String[] { "content_id" });
		detailFieldsMap.put("100015", new String[] { "search_src", "have_result", "search_content" });
		detailFieldsMap.put("100013", new String[] { "module_type", "module_id", "content_type", "content_id" });
		detailFieldsMap.put("101010", new String[] { "content_id", "video_id", "play_type" });
		detailFieldsMap.put("101011", new String[] { "content_id", "video_id" });
		detailFieldsMap.put("101012", new String[] { "content_id", "video_id", "play_time" });
		detailFieldsMap.put("100030", new String[] { "content_id" });
		detailFieldsMap.put("100031", new String[] { "content_id" });
		detailFieldsMap.put("100040", new String[] { "content_id", "share_to" });
		detailFieldsMap.put("100028", new String[] { "content_id", "enter_src", "is_success" });
		detailFieldsMap.put("100041", new String[] { "content_id" });
		detailFieldsMap.put("100042", new String[] { "content_id", "throw_count" });
		detailFieldsMap.put("100050",
				new String[] { "content_id", "video_id", "danmu_type", "danmu_color", "danmu_size", "is_success" });
		detailFieldsMap.put("110002", new String[] { "channel_id" });
		detailFieldsMap.put("110003", new String[] { "child_channel_id" });
		detailFieldsMap.put("300001", new String[] { "page_id", "page_action" });

		detailFieldsMap.put("200004", new String[] {});
		detailFieldsMap.put("200005", new String[] {});
		detailFieldsMap.put("200006", new String[] {});
		detailFieldsMap.put("200007", new String[] {});
		detailFieldsMap.put("200035", new String[] {});
		detailFieldsMap.put("200025", new String[] {});
		detailFieldsMap.put("200023", new String[] {});
		detailFieldsMap.put("200020", new String[] {});
		detailFieldsMap.put("200018", new String[] {});
		detailFieldsMap.put("200026", new String[] {});
		detailFieldsMap.put("500001", new String[] {});
		detailFieldsMap.put("500002", new String[] {});
		detailFieldsMap.put("500003", new String[] {});
	}

	public List<Event> getEvents(HttpServletRequest request) throws Exception {
		StringBuffer jb = new StringBuffer();
		String line = null;
		String realIpAddress = request.getHeader("X-Real-IP");
		BufferedReader reader = null;
		try {
			reader = request.getReader();
			while ((line = reader.readLine()) != null)
				jb.append(line);
			LOG.info("APP上报的加密字符串：" + jb.toString());
		} catch (Exception ex) {
			LOG.error("读取BufferedReader失败:" + ex.getMessage());
			throw new Exception("读取BufferedReader失败:" + ex.getMessage());
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

		this.addRealIpForEachJson(jsonList, realIpAddress);

		return this.convertAppJsonListToEvents(jsonList);
	}

	/**
	 * 添加ip字段
	 * 
	 * @param jsonList
	 * @param ip
	 */

	private void addRealIpForEachJson(List<Map<String, String>> jsonList, String ip) {
		for (int i = 0; i < jsonList.size(); i++) {
			jsonList.get(i).put("ip", ip);
		}
	}

	/**
	 * json字符串转成flume event并设置event的header
	 * 
	 * @param json
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	private List<Event> convertAppJsonListToEvents(List<Map<String, String>> jsonList)
			throws UnsupportedEncodingException {

		List<Event> events = new ArrayList<Event>(jsonList.size());
		for (Map<String, String> jsonMap : jsonList) {
			events.add(this.buildAppJsonEvent(jsonMap));
		}
		return events;
	}

	/**
	 * 构建event 添加header(业务类型)
	 * 
	 * @param eventString
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	private Event buildAppJsonEvent(Map<String, String> eventMap) {
		
		String eventId = eventMap.get(AcfunMaidianConstants.APP_JSONK_EVENT_ID);

		HashMap<String, String> headerMap = new HashMap<String, String>();
		headerMap.put(AcfunMaidianConstants.BIZTYPE, AcfunMaidianConstants.APP);

		StringBuffer sb = new StringBuffer();

		//设置公共字段
		for (String string : commonFields) {
			sb.append(eventMap.get(string) + "\t");
		}

		String[] detailFields = detailFieldsMap.get(eventId);

		//根据是否是sessionlog对个性化字段做处理
		if (eventId.equals(AcfunMaidianConstants.APP_JSONV_SESSION_EVENT_ID)) {

			headerMap.put(AcfunMaidianConstants.LOGTYPE, AcfunMaidianConstants.SESSIONLOG);

			for (String string : detailFields) {
				sb.append(eventMap.get(string) + "\t");
			}

			return EventBuilder.withBody(StringUtils.substringBeforeLast(sb.toString(), "\t").getBytes(), headerMap);
		} else {

			headerMap.put(AcfunMaidianConstants.LOGTYPE, AcfunMaidianConstants.EVENTLOG);

			Map<String, String> detailMap = new HashMap<String, String>();

			for (String string : detailFields) {
				detailMap.put(string, eventMap.get(string));
			}

			sb.append(gson.toJson(detailMap));

			return EventBuilder.withBody(sb.toString().getBytes(), headerMap);
		}

	}

	public void configure(Context context) {

	}

	// public static void main(String[] args) throws Exception {
	// Map<String,String> detailMap = new HashMap<String,String>();
	// detailMap.put("aaa", "aaaa");
	// detailMap.put("bbb", "aaaa");
	// detailMap.put("ccc", "aaaa");
	// System.out.println(gson.toJson(detailMap));
	// }

}

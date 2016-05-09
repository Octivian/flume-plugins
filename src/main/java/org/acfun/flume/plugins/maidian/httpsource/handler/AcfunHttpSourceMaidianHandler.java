package org.acfun.flume.plugins.maidian.httpsource.handler;

import java.io.BufferedReader;
import java.lang.reflect.Type;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.acfun.flume.plugins.utils.AcfunCodecUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

public  class AcfunHttpSourceMaidianHandler implements HTTPSourceHandler {

	protected static final String GET = "GET", POST = "POST", H5 = "h5", APP = "APP", WEB = "WEB";

	private final Gson gson;

	public AcfunHttpSourceMaidianHandler() {
		gson = new GsonBuilder().disableHtmlEscaping().create();
	}
	
	protected static final Logger LOG = LoggerFactory.getLogger(AcfunHttpSourceMaidianHandler.class);
	protected final Type listType = new TypeToken<List<JSONEvent>>() {}.getType();

	public  List<Event> getEvents(HttpServletRequest request) throws Exception{
		String httpType = getHttpType(request);
		if(httpType.equals(APP)){
			return handleAppEvents(request);
		}else if(httpType.equals(WEB)){
			
		}else if(httpType.equals(H5)){
			
		}
		return null;
	}
	
	/**
	 * 处理APP上报JSON方法
	 * @param request
	 * @return
	 * @throws Exception
	 */
	public  List<Event> handleAppEvents(HttpServletRequest request) throws Exception{
		StringBuffer jb = new StringBuffer();
		String line = null;
		BufferedReader reader = request.getReader();
		while ((line = reader.readLine()) != null)
			jb.append(line);
		String json = AcfunCodecUtils.desDecryptionAndUncompressGzip(jb.toString(), "Acfun!@#");
		LOG.info(json);
		String charset = request.getCharacterEncoding();
		// UTF-8 is default for JSON. If no charset is specified, UTF-8 is to
		// be assumed.
		if (charset == null) {
			LOG.debug("Charset is null, default charset of UTF-8 will be used.");
			charset = "UTF-8";
		} else if (!(charset.equalsIgnoreCase("utf-8") || charset.equalsIgnoreCase("utf-16")
				|| charset.equalsIgnoreCase("utf-32"))) {
			LOG.error("Unsupported character set in request {}. " + "JSON handler supports UTF-8, "
					+ "UTF-16 and UTF-32 only.", charset);
			throw new UnsupportedCharsetException("JSON handler supports UTF-8, " + "UTF-16 and UTF-32 only.");
		}

		/*
		 * Gson throws Exception if the data is not parseable to JSON. Need not
		 * catch it since the source will catch it and return error.
		 */
		List<Event> eventList = new ArrayList<Event>(0);
		try {
			eventList = this.buildEventList(json);
			LOG.info("WANGJING-SOHO:JSON-SIZE:"+eventList.size());
		} catch (JsonSyntaxException ex) {
			throw new HTTPBadRequestException("Request has invalid JSON Syntax.", ex);
		}

		for (Event e : eventList) {
			((JSONEvent) e).setCharset(charset);
		}
		return getSimpleEvents(eventList);
	}
	
	private List<Event> buildEventList(String json){
		if(!json.startsWith("[")){
			json="["+json;
		}
		if(!json.endsWith("]")){
			json=json+"]";
		}
		return gson.fromJson(json, listType);
	}

	public void configure(Context context) {
	}

	private List<Event> getSimpleEvents(List<Event> events) {
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
//		String msg = "[{\"device_id\":\"abc123\",\"uid\":\"aaa\"},{\"device_id\":\"bbb123\",\"uid\":\"bbb\"}]";
//		String coded = "27226135CF26DC3FB147C7D14A6185646F1A795E9E87D386EC707F6299B7499163560B0F7143834470C6F69CB1C5F5A1FCB597E11886427B512BB3032255177E76DB60C789344B3E9AAAFD18A5B8B30406840D2A1D0E57E11552115CBF55E62E34A712F52BF99FA9BB44E7C29E48BF8A6146935EABB2D1DB4B4421D9160C59DF7D3D5E8661DBB7ADCF981C99683C804E16E3C6AAC0565A4FA29B57315442EC8AC447D7F3C19046AAF64DB1D2DBC591286EC37AC2C49682CA0F803486FB75B09D7DFA8CDB954CD7476349A95AFD9020B1C8DD2ED297D0534B04F7C7A8E50FB8F77AC11C8C13080F411B50D84D9ADD4C8433ECEE87591504C49F434C776C9BD8ED150E25A4F8E1C139869AAD83628416DD35DD6096DC00CD1E1154B4A060CEBEB60F4DB9F3B97D53E940B547682675CDF2D6E0BCA340B0F717D8EC4AA0F0B690E84EC070AA0877BAE4DDB34E459D2707334B323F63D4106CEFEC0E2502BEA1CABB2147A67223B1B83E37E5587E4D48FC8F4016AA0F051AF9DC50EBAF56930ED3D1BB147520E9A0F4533DDE97E8A3BDE84910282A8AF0B9326C8FABFEDBA7EBF39871D8B727FB5C4238F1491AB8E8AC1C825E66AF2CD9DBD5C9E4728BFF645F3B1D4149BD160A094CD2C0681EAADA718163652C062AC7EB51F9564DEC8336810C294D75FE41198C650591DC52D618878AC54D878115445244CBDB6E06FB8FADFBBDD52B250E66A40B76F8F5870B287175005CD23A2EC674DA52DC98AD2C76989BC981EDF683A71913C028F8F42305D8CED17CD1723CDEA75794293B926AAE8993B7F4AB284BC20938C94C190C27CA22CA0318078AE898590C56814B08534224B5512908A758F60F1CBE020DA33143977AD5871268C261CEB41A4BF6818F46F102AE86E140FC9806DCB584D7169E3B43331AD15B7859EAA2273E60D56C526A9AD952FB1130A8E786F5A1603D2F0A0A199EB7738B4162CB278273B22C10E153E1631596535167585E9DEBFBF4E231401934B4EBE2630B7C1C60214255A8639118591D9AAB11B3491C74C9EC44F10413328AC6AF04E4B535795B1E395119C7BE6F871B92D49BF09A40C642D5BB39AE45A1B10EEA5C02263F38524EAA28A394822852237145D958AD3745AD6BCA6D5A0A7963E707623AAA3D97B33B71C0764B938AC8C7DD324B1975A6ACC1CCC83D4DA3ACE70AF4E63A3BF39C91419A29F6E0293AA7126CE00BF38406FDF1C3195AE9CCD23C0BA3E4095195A743F2A73AB318448F2EB674928C29EF6942EF0135A099F6954CBA56C8751C863DEA945E8F805D7E55B7AAD98F21576ADC44AB23A1184260D68DAE";
		String coded1 = "27226135 cf26dc3f 26fb1e86 dda7a543 9a8131ad 65c1045c f06d0731 96b68cbd 2e3d2852 be1db9fc 505e52b3 30ba2503 58a45b42 253fdeb0 338321a7 4f0ed111 1ed6dd7f 12d29217 a6c8bcb2 cd4aed09 adb0d4d2 68b5c27c b9ee792f a6d42404 8c9a0869 015d738b 2748bd40 a8709fb9 48ca2670 bc6018f4 1bac77a8 6a848fa2 83899701 4251f185 5e0b2fee a899a1b1 87594072 d5ea9987 9c6abf7b 5ea40fb8 e2b1966e 36bbac61 e925b426 f9ff0043 5b02b7f1 7513c544 e60b1eeb 53a50b22 42e7fdc3 4f79b152 6b941f9a 6ce42f8f 630ac365 298dbf88 a1ce0dad 753947cb 7dcf5429 d80675a4 5ee365a0 0d4afef2 49975407 8954eca7 1ec20091 f149f48e fc58e10c 05533e6b";
		coded1 = coded1.replace(" ","");
		System.out.println(coded1);
//		System.out.println(new String(AcfunCodecUtils.compressGzipAndDesEncryption(msg, "Acfun!@#")));
		System.out.println(AcfunCodecUtils.desDecryptionAndUncompressGzip(coded1,"Acfun!@#"));
//		System.out.println(CodecUtils
//				.desDecryptionAndUncompressGzip(CodecUtils.compressGzipAndDesEncryption(msg, "Acfun!@#"), "Acfun!@#"));
		
//		System.out.println(new String(CodecUtils.encrypt(msg.getBytes("UTF-8"),"Acfun!@#")));
	}
}

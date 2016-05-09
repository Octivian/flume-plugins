package org.acfun.flume.plugins.httpsource.handler.maidian;

import java.io.BufferedReader;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.acfun.flume.plugins.utils.CodecUtils;
import org.apache.flume.Event;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.source.http.HTTPBadRequestException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

public class AcfunHttpSouceAppHandler extends AcfunHttpSourceMaidianHandler {

	private final Gson gson;

	public AcfunHttpSouceAppHandler() {
		gson = new GsonBuilder().disableHtmlEscaping().create();
	}

	@Override
	public List<Event> handleEvents(HttpServletRequest request) throws Exception {
		StringBuffer jb = new StringBuffer();
		String line = null;
		BufferedReader reader = request.getReader();
		while ((line = reader.readLine()) != null)
			jb.append(line);
		String json = CodecUtils.desDecryptionAndUncompressGzip(jb.toString(), "Acfun!@#");
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
			eventList = gson.fromJson(json, listType);
		} catch (JsonSyntaxException ex) {
			throw new HTTPBadRequestException("Request has invalid JSON Syntax.", ex);
		}

		for (Event e : eventList) {
			((JSONEvent) e).setCharset(charset);
		}
		return getSimpleEvents(eventList);
	}

}

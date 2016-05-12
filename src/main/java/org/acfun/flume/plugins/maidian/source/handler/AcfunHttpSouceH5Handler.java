package org.acfun.flume.plugins.maidian.source.handler;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.acfun.flume.plugins.maidian.source.constant.AcfunHttpSourceConstants;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AcfunHttpSouceH5Handler implements HTTPSourceHandler {
	
	
	protected static final Logger LOG = LoggerFactory.getLogger(AcfunHttpSouceH5Handler.class);

	public void configure(Context context) {}

	public List<Event> getEvents(HttpServletRequest request) throws HTTPBadRequestException, Exception {
		
		String queryString = request.getQueryString();
		
		LOG.info("H5端获取的数据"+queryString);
		
		HashMap<String, String> headerMap = new HashMap<String, String>();
		headerMap.put(AcfunHttpSourceConstants.TIMESTAMP, String.valueOf(new Date().getTime()));
		headerMap.put(AcfunHttpSourceConstants.BIZTYPE, AcfunHttpSourceConstants.H5);
		List<Event> arrayList = new ArrayList<Event>();
		arrayList.add(EventBuilder.withBody(queryString.getBytes("UTF-8"), headerMap));
		return arrayList;
	}

}

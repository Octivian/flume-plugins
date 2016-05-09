package org.acfun.flume.plugins.httpsource.handler.maidian;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.flume.Event;
import org.apache.flume.source.http.HTTPSourceHandler;

public interface IAcfunHttpSourceHandler extends HTTPSourceHandler{

	List<Event> handleRequestByType(HttpServletRequest request);
}

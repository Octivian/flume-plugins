package org.acfun.flume.plugins.maidian.constant;

/**
 *AcfunHttpSource相关常量
 */
public class AcfunMaidianConstants {

  public static final String CONFIG_PORT = "port";
  public static final String CONFIG_HANDLERS = "handler";
  
  public static final String CONFIG_HANDLERS_CLASS = "class";
  
  public static final String CONFIG_HANDLERS_CONF_PATH = "confpath";
  
  public static final String CONFIG_BIND = "bind";

  public static final String DEFAULT_BIND = "0.0.0.0";

  public static final String SSL_KEYSTORE = "keystore";
  public static final String SSL_KEYSTORE_PASSWORD = "keystorePassword";
  public static final String SSL_ENABLED = "enableSSL";
  public static final String EXCLUDE_PROTOCOLS = "excludeProtocols";
  
  public static final String CONFIG_HANDLERS_MAP_SEPRATOR = ",";
  
  public static final String CONFIG_HANDLERS_ENTRY_SEPRATOR = ":";
  
  public static final String APP = "app";
  
  public static final String WEB = "web";
  
  public static final String H5 = "h5";
  
  public static final String REQUEST_TYPE_GET = "GET";
  
  public static final String REQUEST_TYPE_POST = "POST";
  
  public static final String BIZTYPE = "biztype";
  
  public static final String LOGTYPE = "logtype";
  
  public static final String EVENTLOG = "event";
  
  public static final String SESSIONLOG = "session";
  
  public static final String TIMESTAMP = "timestamp";
  
  public static final String APP_JSONK_TIME = "time";
  
  public static final String APP_JSONK_IMEI = "IMEI";
  
  public static final String APP_JSONK_EVENT_ID = "event_id";
  
  public static final String APP_JSONK_BURY_VERSION = "bury_version";
  
  public static final String APP_JSONK_DEVICE_ID = "device_id";
  
  public static final String APP_JSONV_SESSION_EVENT_ID = "100101";
  
  public static final String GET_MAIDIAN_LOG_REGEX = ",(?=[^\\}\\}]*(\\{\\{|$))";
  
}
package cn.thj.flume.handler;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.nio.charset.UnsupportedCharsetException;
import java.text.SimpleDateFormat;
import java.util.*;

import static cn.thj.flume.util.JsonUtil.getJsonNode;
import static cn.thj.flume.util.StringUtil.isEmpty;


/**
 * 日志 handler
 */
public class LogHandler implements HTTPSourceHandler {

    private static final Logger logger = LoggerFactory.getLogger(LogHandler.class);

    private static final String DEFAULT_CHARSET = "UTF-8";
    private final static String DATE_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public void configure(Context context) {
    }

    public List<Event> getEvents(HttpServletRequest request) throws Exception {

        String charset = request.getCharacterEncoding();
        charset = validateCharSet(charset);
        List<Event> eventList = new ArrayList<Event>();
        //此处用的是get请求 p 为参数
        String body = request.getParameter("p");
        logger.debug(body);
        if (!isEmpty(body)) {
            int serviceType = getServiceType(body);
            Event e = new SimpleEvent();
            e.setBody(getBodyData(body, charset, serviceType));
            String host = request.getRemoteHost();
            Map<String, String> headers = addHeaders(host, serviceType);
            e.setHeaders(headers);
            eventList.add(e);
        }
        return eventList;
    }



    // add headers
    private Map<String, String> addHeaders(String host, int serviceType) {

        Map<String, String> headers = new HashMap<String, String>();
        //add current time
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT_PATTERN, Locale.CHINA);
        String timestamp = format.format(new Date());

        headers.put("type", "" + serviceType);
        headers.put("host", host);
        headers.put("timestamp", timestamp);
        return headers;
    }

    // validate charset
    private String validateCharSet(String charset) {
        if (charset == null) {
            logger.debug("Charset is null, default charset of UTF-8 will be used.");
            charset = DEFAULT_CHARSET;
        } else if ((!charset.equalsIgnoreCase("utf-8"))
                && (!charset.equalsIgnoreCase("utf-16"))
                && (!charset.equalsIgnoreCase("utf-32"))) {
            logger.error("Unsupported character set in request {}. YunlaiLogHandler supports UTF-8, UTF-16 and UTF-32 only.", charset);
            throw new UnsupportedCharsetException("YunlaiLogHandler supports UTF-8, UTF-16 and UTF-32 only.");
        }
        return charset;
    }

    // get serviceType
    private int getServiceType(String body) {
        int type = 0;
        try {
            type = getJsonNode(body).get("opt").get("service_type").asInt(0);
        } catch (Exception e) {
            logger.error("service type:", e);
        }
        return type;
    }

    private byte[] getBodyData(String body, String charset, int service_type) throws Exception {

        byte[] data = null;
        data = body.getBytes(charset);
        if (service_type > 0) {
            data = getJsonNode(body).get("data").toString().getBytes(charset);
        }
        return data;
    }

}
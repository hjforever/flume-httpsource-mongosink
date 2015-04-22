package cn.thj.flume.util;

import cn.thj.flume.util.CustomObjectMapper;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;


/**
 *
 * json util
 */
public class JsonUtil {

    private static Logger logger = LoggerFactory.getLogger(JsonUtil.class);
    public static final CustomObjectMapper mapper;

    static {
        mapper = new CustomObjectMapper();
    }

    public static String toJSONString(Object object) {

        StringWriter writer = new StringWriter();
        try {
            mapper.writeValue(writer, object);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Unable to serialize to json: " + object, e);
            return null;
        }
        return writer.toString();
    }

    public static <T> T parse(String json, Class<T> type) {
        T object;
        try {
            object = mapper.readValue(json, type);
        } catch (RuntimeException e) {
            logger.error("Runtime exception during deserializing ");
            throw e;
        } catch (Exception e) {
            logger.error("exception during deserializing[" + json + "]", e);
            return null;
        }
        return object;
    }

    public static List<Map<String, Object>> toArray(JsonParser jp) {

        try {
            return mapper.readValue(jp, new TypeReference<List<Map<String, Object>>>() {
            });
        } catch (Exception e) {
            logger.error("Runtime exception ", e);
        }
        return null;
    }

    public static <T> List<T> toArray(JsonParser jp, Class<T> type) {

        try {
            return mapper.readValue(jp, new TypeReference<List<T>>() {
            });
        } catch (Exception e) {
            logger.error("Runtime exception ", e);
        }
        return null;
    }

    public static List<Map<String, Object>> toArray(String jsonArrayString) {

        try {
            return mapper.readValue(jsonArrayString, new TypeReference<List<Map<String, Object>>>() {
            });
        } catch (Exception e) {
            logger.error("Runtime exception", e);
        }
        return null;
    }

    public static JsonNode getJsonNode(String jsonString) {
        try {
            return mapper.readTree(jsonString);
        } catch (IOException e) {
            logger.error("Runtime exception", e);
        }
        return null;
    }
}

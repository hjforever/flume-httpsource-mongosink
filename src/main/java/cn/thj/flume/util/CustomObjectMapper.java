package cn.thj.flume.util;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 *
 *
 * custom mapper
 *
 */
public class CustomObjectMapper extends ObjectMapper {

    private static final long serialVersionUID = -6309566588474236465L;

    public CustomObjectMapper() {
        super();
        this.getSerializerProvider().setNullValueSerializer(new JsonSerializer<Object>() {
            public void serialize(Object ob, JsonGenerator jg, SerializerProvider sp) throws IOException, JsonProcessingException {
                jg.writeString("");
            }
        });
        this.configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, true);
        this.configure(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED, false);
        // 设置输入:禁止把POJO中值为null的字段映射到json字符串中
        this.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        // 这里的配置根据实际情况增减。
        this.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // 设置输出:包含的属性不能为空
        this.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, false);
        this.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        this.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    }
}
package cn.thj.flume.util;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 * db properties util
 *
 */
public class DBPropertiesUtil {

    private static Logger logger = LoggerFactory.getLogger(DBPropertiesUtil.class);

    private static final String DEFAULT_DB_PROPERTIES_NAME = "db.properties";

    private static PropertiesConfiguration config  ;

    static {
        try {
            config = new PropertiesConfiguration(DEFAULT_DB_PROPERTIES_NAME);
        } catch (ConfigurationException e) {
            logger.error("RuntimeException:",e);
        }
        FileChangedReloadingStrategy fileChangedReloadingStrategy = new FileChangedReloadingStrategy();
        fileChangedReloadingStrategy.setRefreshDelay(1000);
        config.setReloadingStrategy(fileChangedReloadingStrategy);
    }

    public static String getString(String key) {
        return config.getString(key);
    }

    public static String getStringDefault(String key, String defaultValue) {
        return config.getString(key, defaultValue);
    }

}

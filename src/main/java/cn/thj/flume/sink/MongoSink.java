package cn.thj.flume.sink;


import cn.thj.flume.util.DBPropertiesUtil;
import cn.thj.flume.util.JsonUtil;
import com.mongodb.*;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

public class MongoSink extends AbstractSink implements Configurable {

    private static Logger logger = LoggerFactory.getLogger(MongoSink.class);

    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String AUTHENTICATION_ENABLED = "authenticationEnabled";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String DB_NAME = "db";
    public static final String COLLECTION = "collection";
    public static final String NAME_PREFIX = "MongSink_";
    public static final String BATCH_SIZE = "batch";
    public static final boolean DEFAULT_AUTHENTICATION_ENABLED = false;
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 27017;
    public static final String DEFAULT_DB = "events";
    public static final String DEFAULT_COLLECTION = "events";
    public static final int DEFAULT_BATCH = 1000;
    public static final String DEFAULT_ENCODE = "UTF-8";
    private static AtomicInteger counter = new AtomicInteger();
    private MongoClient mongoClient;
    private DB db;
    private String host;
    private int port;
    private boolean authentication_enabled;
    private String username;


    private String password;
    private String dbName;
    //配置中默认的collection
    private String collectionName;
    private int batchSize;


    public void configure(Context context) {

        setName("MongSink_" + counter.getAndIncrement());
        host = context.getString("host", "localhost");
        port = context.getInteger("port", Integer.valueOf(27017));
        this.authentication_enabled = context.getBoolean("authenticationEnabled", Boolean.valueOf(false)).booleanValue();
        if (this.authentication_enabled) {
            this.username = context.getString("username");
            this.password = context.getString("password");
        } else {
            this.username = "";
            this.password = "";
        }
        this.dbName = context.getString("db", "events");
        this.collectionName = context.getString("collection", "events");
        this.batchSize = context.getInteger("batch", Integer.valueOf(1000)).intValue();

    }

    public synchronized void start() {
        logger.info("Starting {}...", getName());
        try {
            ServerAddress addr = new ServerAddress(this.host, this.port);
            List<MongoCredential> credentialsList = new ArrayList<MongoCredential>();
            MongoCredential cre = MongoCredential.createMongoCRCredential(this.username, this.dbName, this.password.toCharArray());
            credentialsList.add(cre);
            this.mongoClient = new MongoClient(addr, credentialsList);
            this.db = this.mongoClient.getDB(this.dbName);
        } catch (UnknownHostException e) {
            logger.error("Can't connect to mongoDB", e);
            return;
        }
        super.start();
        logger.info("Started {}.", getName());
    }

    public Sink.Status process() throws EventDeliveryException {
        logger.debug("{} start to process event", getName());

        Sink.Status status = Sink.Status.READY;
        Channel channel = getChannel();

        Map<String, List<DBObject>> dsMap = new HashMap<String, List<DBObject>>();
        Transaction tx = null;
        try {
            tx = channel.getTransaction();
            tx.begin();
            for (int i = 0; i < this.batchSize; i++) {
                Event event = channel.take();
                if (event == null) {
                    break;
                }
                addEventToMap(dsMap, event);
            }
            if ((dsMap != null) && (!dsMap.isEmpty())) {
                logger.debug("map size : {} ", dsMap.size());
                Iterator<Entry<String, List<DBObject>>> iter = dsMap.entrySet().iterator();
                Entry<String, List<DBObject>> entry;
                while (iter.hasNext()) {
                    entry = iter.next();
                    String key = this.dbName + ".service_type." + entry.getKey();
                    String collection = DBPropertiesUtil.getStringDefault(key, collectionName);
                    DBCollection coll = this.db.getCollection(collection);
                    coll.insert(entry.getValue());
                }
            } else {
                status = Sink.Status.BACKOFF;
            }
            tx.commit();
        } catch (Exception e) {
            if (tx != null) {
                tx.rollback();
            }
            logger.error("can't process events !!!", e);
            throw new EventDeliveryException("Failed to send events", e);
        } finally {
            if (tx != null) {
                tx.close();
            }
            dsMap = null;
        }
        return status;
    }

    /**
     * 将 envet根据serviceType添加到map中
     *
     * @param dsMap
     * @param event
     */
    private void addEventToMap(Map<String, List<DBObject>> dsMap, Event event) {
        try {
            if (event == null || event.getBody() == null || event.getBody().length == 0) {
                return;
            }
            byte[] body = event.getBody();
            Map<String, String> headers = event.getHeaders();
            if (!headers.containsKey("type")) {
                return;
            }
            String serviceType = headers.get("type");
            String bodyStr = new String(body, "UTF-8");
            Map<String, Object> data = JsonUtil.parse(bodyStr, Map.class);
            if (data == null || data.isEmpty()) {
                return;
            }
            Date date = new Date();
            data.put("stime", date);
            DBObject eventJson = new BasicDBObject(data);
            if (dsMap.containsKey(serviceType)) {
                dsMap.get(serviceType).add(eventJson);
            } else {
                List<DBObject> dbObjectList = new ArrayList<DBObject>();
                dbObjectList.add(eventJson);
                dsMap.put(serviceType, dbObjectList);
            }
        } catch (UnsupportedEncodingException e) {
            logger.error("UnsupportedEncodingException:", e);
        }
    }

}
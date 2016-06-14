package com.redborder.kafkaproducer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProducerThread extends Thread {
    Random randomX = new Random();
    boolean run = true;
    int events;
    long time = 0;
    int times = 0;
    String _brokerList = new String("");
    Integer id;
    boolean enrich = true;

    List<String> topicsList = null;
    Producer<String, String> producer;
    String zookeeper;

    String topics;
    int index = 0;
    Map<String, String> tiers = new HashMap<String, String>();
    Map<String, Integer> sensorId = new HashMap<String, Integer>();
    Map<String, String> sha256MalwareNames = new HashMap<String, String>();

    public ProducerThread(String zookeeper, String topic, String brokerList, Integer events, Integer id, boolean enrich) {
        this.zookeeper = zookeeper;
        this.topics = topic;
        this._brokerList = brokerList;
        this.events = events;
        this.id = id;
        this.enrich = enrich;
        tiers.put("11111111", "gold");
        tiers.put("22222222", "silver");
        tiers.put("33333333", "silver");
        tiers.put("44444444", "silver");
        sensorId.put("11111111", 2);
        sensorId.put("22222222", 5);
        sensorId.put("33333333", 6);
        sensorId.put("44444444", 7);
    }

    public void terminate() {
        run = false;
        producer.close();
    }

    public void run() {

        if (_brokerList.equals("")) {
            configProducer(zookeeper, false);
        } else {
            configProducer(zookeeper, true);
        }


        System.out.printf("[ ThreadNumber: %3d ] Producing to:  %s \n", id, _brokerList);


        if (topics.contains(","))
            topicsList = Arrays.asList(topics.split(","));
        else if (topics.contains(":"))
            topicsList = Arrays.asList(topics.split(":"));
        else {
            topicsList = new ArrayList<String>();
            topicsList.add(topics);
        }

        if (events < 1000 && events != 0) {
            time = 1000 / events;
        } else if (events != 0) {
            times = events / 1000;
        } else
            time = 0;

        long metrics = 0;
        long timeSeconds = System.currentTimeMillis() / 1000;
        long newTimeSeconds = 0;

        while (run) {
            if (topicsList.contains("rb_flow")) {
                producer.send(getFlow());

                metrics++;
                newTimeSeconds = System.currentTimeMillis() / 1000;
                if ((timeSeconds + 10) <= newTimeSeconds) {
                    timeSeconds = newTimeSeconds;
                    System.out.printf("[ ThreadNumber: %3d ] Flows/sec:  %5d \n", id, (metrics / 10));
                    metrics = 0;
                }
            }

            if (topicsList.contains("rb_loc")) {
                producer.send(getLocation());
            }

            if (topicsList.contains("rb_loc10")) {
                producer.send(getMse10());
            }

            if (topicsList.contains("rb_social")) {
                producer.send(getSocial());
            }

            if (topicsList.contains("rb_event")) {
                producer.send(getEvent("ABCDEFG"));
            }

            if (topicsList.contains("rb_malware")) {
                String sha256 = getSha256();
                String malware = getMalwareName();

                if (!sha256MalwareNames.containsKey(sha256) || sha256MalwareNames.get(sha256).equals("clean")) {
                    sha256MalwareNames.put(sha256, malware);
                }

                producer.send(getEvent(sha256));
                producer.send(getMalware(sha256));
            }

            if (topicsList.contains("rb_mail")) {
                producer.send(getMail());
            }

            if (times == 0) {
                try {
                    Thread.sleep(time);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                index++;
            }

            if (times < index) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                index = 0;
            }

        }
    }

    public void configProducer(String zookeeper, boolean broker) {

        if (!broker) {

            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeper, retryPolicy);
            client.start();

            List<String> ids = null;
            boolean first = true;

            try {
                ids = client.getChildren().forPath("/brokers/ids");
            } catch (Exception ex) {
                Logger.getLogger(FlowsProducer.class.getName()).log(Level.SEVERE, null, ex);
            }

            for (String id : ids) {
                String jsonString = null;

                try {
                    jsonString = new String(client.getData().forPath("/brokers/ids/" + id), "UTF-8");
                } catch (Exception ex) {
                    Logger.getLogger(FlowsProducer.class.getName()).log(Level.SEVERE, null, ex);
                }

                if (jsonString != null) {
                    ObjectMapper mapper = new ObjectMapper();
                    Map<String, Object> json = null;

                    try {
                        json = mapper.readValue(jsonString, Map.class);

                        if (first) {
                            _brokerList = _brokerList.concat(json.get("host") + ":" + json.get("port"));
                            first = false;
                        } else {
                            _brokerList = _brokerList.concat("," + json.get("host") + ":" + json.get("port"));
                        }
                    } catch (NullPointerException e) {
                        Logger.getLogger(FlowsProducer.class.getName()).log(Level.SEVERE, "Failed converting a JSON tuple to a Map class", e);
                    } catch (JsonMappingException e) {
                        Logger.getLogger(FlowsProducer.class.getName()).log(Level.SEVERE, "Failed converting a JSON tuple to a Map class", e);
                    } catch (JsonParseException e) {
                        Logger.getLogger(FlowsProducer.class.getName()).log(Level.SEVERE, "Failed converting a JSON tuple to a Map class", e);
                    } catch (IOException e) {
                        Logger.getLogger(FlowsProducer.class.getName()).log(Level.SEVERE, "Failed converting a JSON tuple to a Map class", e);
                    }
                }
            }
            client.close();
        }
        Properties props = new Properties();
        props.put("metadata.broker.list", _brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        props.put("message.send.max.retries", "60");
        props.put("retry.backoff.ms", "1000");
        props.put("producer.type", "async");
        props.put("queue.buffering.max.messages", "10000");
        props.put("queue.buffering.max.ms", "500");
        props.put("partitioner.class", "com.redborder.kafkaproducer.SimplePartitioner");

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);

    }

    public static Random randomHex = new Random();

    public static Integer getHex() {
        int idx = randomHex.nextInt(0xF);
        return idx;
    }

    public static String getMalwareName() {
        String[] malwares = {"clean", "trojan4x", "por56X", "win32.da13j"};
        int idx = randomHex.nextInt(malwares.length);
        return malwares[idx];
    }

    public static String getMac() {
        String mac = "00" + ":" +
                "00" + ":" +
                "00" + ":" +
                "00" + ":" +
                "0" + String.format("%x", getHex()) + ":" +
                String.format("%x", getHex()) + String.format("%x", getHex());

        return mac;
    }

    public static String getAPmac() {
        String[] macs = {"11:11:11:11:11:11", "22:22:22:22:22:22",
                "33:33:33:33:33:33"};

        int idx = new Random().nextInt(macs.length);
        return macs[idx];
    }

    public static String getIP() {
        String ip = "192.168.";
        Integer i = new Random().nextInt(255);
        ip = ip + i + ".";
        i = new Random().nextInt(255);
        ip = ip + i;

        return ip;
    }

    public static String getSSID() {
        String[] ip = {"ssid_A", "ssid_B", "ssid_C"};

        int ips = new Random().nextInt(ip.length);

        return ip[ips];
    }

    public static String namespace() {
        String[] namespaces = {
                "11111111"
        };

        int index = new Random().nextInt(namespaces.length);

        return namespaces[index];
    }

    public KeyedMessage getMse10() {


        String client_mac = getMac();

        String mse1 = "{\"notifications\":[{\"notificationType\":\"locationupdate\"," +
                "\"subscriptionName\":\"motus-MSE-Alpha80\",\"entity\":\"WIRELESS_CLIENTS\"," +
                "\"deviceId\":\"" + client_mac + "\",\"lastSeen\":\"2015-02-24T08:45:48.154+0000\"," +
                "\"ssid\":\"ssid-test\",\"band\":null,\"apMacAddress\":\"" + getAPmac() + "\"," +
                "\"locationMapHierarchy\":\"Motus>East>G\"," +
                "\"locationCoordinate\":{\"x\":147.49353,\"y\":125.65644,\"z\":0.0," +
                "\"unit\":\"FEET\"},\"confidenceFactor\":24.0,\"timestamp\":" + System.currentTimeMillis() + "}]}";

        String mse2 = "{\"notifications\":[{\"notificationType\":\"locationupdate\"," +
                "\"subscriptionName\":\"motus-MSE-Alpha80\",\"entity\":\"WIRELESS_CLIENTS\"," +
                "\"deviceId\":\"" + client_mac + "\",\"lastSeen\":\"2015-02-24T08:45:48.154+0000\"," +
                "\"ssid\":\"ssid-test\",\"band\":null,\"apMacAddress\":\"" + getAPmac() + "\"," +
                "\"locationMapHierarchy\":\"Motus>Bottom Bar>G\"," +
                "\"locationCoordinate\":{\"x\":147.49353,\"y\":125.65644,\"z\":0.0," +
                "\"unit\":\"FEET\"},\"confidenceFactor\":24.0,\"timestamp\":" + System.currentTimeMillis() + "}]}";

        String[] mse = new String[]{mse1, mse2};

        return new KeyedMessage<String, String>("rb_loc", client_mac, mse[randomX.nextInt(mse.length)]);
    }

    public KeyedMessage getLocation() {
        String[] status = {"ASSOCIATED", "PROBING", "UNKNOWN"};
        int statusInt = randomX.nextInt(status.length);

        String[] zonas = {"Zone X", "Zone C", "Zone A", "Zone B",
                "Zone Y"};
        int zonaInt = randomX.nextInt(zonas.length);


        double lat = 37.40;
        double lon = 26.97;

        double random = randomX.nextInt(100);
        double random2 = randomX.nextInt(100);
        double randomOk = random / 100;
        double random1k = random2 / 100;

        String client_mac = getMac();

        DateTime date = new DateTime();

        String loc = "{\"StreamingNotification\":{\"subscriptionName\":\"ENEO_STREAM_JSON_TCP\",\"entity\":\"WIRELESS_CLIENTS\","
                + "\"deviceId\":\"" + client_mac + "\",\"mseUdi\":\"AIR-MSE-3355-K9:V03:KQ4V9TX\",\"floorRefId\":0,"
                + "\"location\":{\"macAddress\":\"" + client_mac + "\",\"mapInfo\""
                + ":{\"mapHierarchyString\":\"" + zonas[zonaInt] + ">" + zonas[zonaInt] + ">" + zonas[zonaInt] + "\","
                + "\"floorRefId\":4698041219291283737,\"floorDimension\":"
                + "{\"length\":160.11,\"width\":414.03,\"height\":10.0,\"offsetX\":0.0,"
                + "\"offsetY\":24.2,\"unit\":\"FEET\"},\"image\":{\"imageName\":\"domain_0_1389315458852.png\"}},"
                + "\"mapCoordinate\":{\"x\":53.17529,\"y\":81.43848,\"unit\":\"FEET\"}"
                + ",\"currentlyTracked\":true,\"confidenceFactor\":40.0,\"statistics\":"
                + "{\"currentServerTime\":\"2014-04-16T04:41:53.256-0700\",\"firstLocatedTime\""
                + ":\"2014-04-15T23:10:36.773-0700\",\"lastLocatedTime\":\"2014-04-16T04:41:20.901-0700\"},\"geoCoordinate\":"
                + "{\"lattitude\":" + (lat + randomOk) + ",\"longitude\":" + (lon - random1k) + ",\"unit\":\"DEGREES\"},\"ipAddress\":[\"10.71.8.182\","
                + "\"fe80:0000:0000:0000:6a09:27ff:fe24:86b8\"],\"ssId\":\"" + getSSID() + "\",\"band\":\"UNKNOWN\",\"apMacAddress\":\"" + getAPmac() + "\","
                + "\"dot11Status\":\"" + status[statusInt] + "\",\"guestUser\":false},"
                + "\"timestamp\":\"" + date.toDateTimeISO().toString() + "\"}}";

        return new KeyedMessage<String, String>("rb_loc", client_mac, loc);
    }

    public Integer getSensorId(String namespace_id) {
        return sensorId.get(namespace_id);
    }

    public KeyedMessage getFlow() {

        String[] status = {"ASSOCIATED", "PROBING", "UNKNOWN"};
        int statusInt = randomX.nextInt(status.length);

        String[] zonas = {"zone_X", "zone_C", "zone_A", "zone_B",
                "zone_Y"};
        int zonaInt = randomX.nextInt(zonas.length);

        String[] apps = {"APP X", "APP C", "APP A", "APP B",
                "APP Y"};

        double lat = 37.40;
        double lon = 26.97;

        double random = randomX.nextInt(100);
        double random2 = randomX.nextInt(100);
        double randomOk = random / 100;
        double random1k = random2 / 100;
        String client_mac = getMac();
        String flow = null;
        String namespace = namespace();

        if (enrich) {

            flow = "{\"client_latlong\":\"" + (lat + randomOk) + "," + (lon - random1k) + "\"," +
                    "\"dst_country_code\":\"US\",\"dot11_status\":\"" + status[statusInt] + "\"," +
                    "\"bytes\":" + new Random().nextInt(3000) + ",\"src_net_name\":\"0.0.0.0/0\",\"flow_sampler_id\":0," +
                    "\"direction\":\"ingress\",\"wireless_station\":\"" + getAPmac() + "\"," +
                    "\"biflow_direction\":\"initiator\",\"pkts\":" + randomX.nextInt(500) + ",\"dst\":\"" + getIP() + "\"," +
                    "\"type\":\"NetFlowv10\",\"campus\":\"" + zonas[zonaInt] + " campus" + "\"," +
                    "\"building\":\"" + zonas[zonaInt] + " building" + "\",\"timestamp\":" + ((System.currentTimeMillis() / 1000)) + "," +
                    "\"client_mac\":\"" + client_mac + "\",\"wireless_id\":\"" + getSSID() + "\"," +
                    "\"flow_end_reason\":\"idle timeout\",\"src_net\":\"0.0.0.0/0\"," +
                    "\"client_rssi_num\":" + (-randomX.nextInt(80)) + ",\"engine_id_name\":\"IANA-L4\"," +
                    "\"src\":\"" + getIP() + "\",\"application_id\":\"" + randomX.nextInt(10) + ":" + randomX.nextInt(100) + "\"," +
                    "\"sensor_ip\":\"90.1.44.3\"," +
                    "\"application_id_name\":\"" + apps[zonaInt] + "\",\"dst_net\":\"0.0.0.0/0\"," +
                    "\"l4_proto\":" + randomX.nextInt(10) + ",\"ip_protocol_version\":4,\"dst_net_name\":\"0.0.0.0/0\"," +
                    "\"sensor_name\":\"sensor_" + zonas[zonaInt] + "_" + tiers.get(namespace) + "\" ,\"src_country_code\":\"US\"," +
                    "\"floor\":\"" + zonas[zonaInt] + " floor" + "\",\"engine_id\":" + randomX.nextInt(20) +
                    ",\"client_mac_vendor\":\"SAMSUNG ELECTRO-MECHANICS\", \"first_switched\": " + ((System.currentTimeMillis() / 1000) - (2 * 60)) +
                    ", \"namespace_uuid\":\"" + namespace + "\", \"tier\":\"" + tiers.get(namespace) + "\", \"sensor_uuid\":" + getSensorId(namespace) + "}";
        } else {

            flow = "{" +
                    "\"bytes\":" + new Random().nextInt(3000) + ",\"src_net_name\":\"0.0.0.0/0\",\"flow_sampler_id\":0," +
                    "\"direction\":\"ingress\"," +
                    "\"biflow_direction\":\"initiator\",\"pkts\":" + randomX.nextInt(500) + ",\"dst\":\"" + getIP() + "\"," +
                    "\"type\":\"NetFlowv10\"," +
                    "\"timestamp\":" + (System.currentTimeMillis() / 1000) + "," +
                    "\"client_mac\":\"" + client_mac + "\"," +
                    "\"flow_end_reason\":\"idle timeout\",\"src_net\":\"0.0.0.0/0\"," +
                    "\"engine_id_name\":\"IANA-L4\"," +
                    "\"src\":\"" + getIP() + "\",\"application_id\":\"" + randomX.nextInt(10) + ":" + randomX.nextInt(100) + "\"," +
                    "\"sensor_ip\":\"90.1.44.3\"," +
                    "\"application_id_name\":\"" + apps[zonaInt] + "\",\"dst_net\":\"0.0.0.0/0\"," +
                    "\"l4_proto\":" + randomX.nextInt(10) + ",\"ip_protocol_version\":4,\"dst_net_name\":\"0.0.0.0/0\"," +
                    "\"sensor_name\":\"sensor_" + namespace + "_" + tiers.get(namespace) + "\" ," +
                    "\"engine_id\":" + randomX.nextInt(20) +
                    ", \"first_switched\": " + ((System.currentTimeMillis() / 1000) - (2 * 60)) + ", \"namespace_uuid\":\"" + namespace + "\", \"tier\":\"" + tiers.get(namespace) +
                    "\", \"sensor_uuid\":" + getSensorId(namespace) + "}";
        }

        return new KeyedMessage<String, String>("rb_flow", client_mac, flow);
    }

    public KeyedMessage getEvent(String sha256) {

        String[] classification = {"Potential Corporate Privacy Violation", "Sensitive Data was Transmitted Across the Network", "Potentially Bad Traffic",
                "Unknown Traffic"};

        String[] msg = {"SERVER-IIS Microsoft IIS 7.5 client verify null pointer attempt", "OS-MOBILE Android User-Agent detected", "sensitive_data: sensitive data - U.S. social security numbers without dashes"
                , "http_inspect: OVERSIZE REQUEST-URI DIRECTORY", "OS-WINDOWS Microsoft Windows UPnP Location overflow attempt", "ssp_ssl: Invalid Client HELLO after Server HELLO Detected"};

        int classificationInt = new Random().nextInt(classification.length);
        int msgInt = new Random().nextInt(msg.length);
        String client_mac = getMac();
        String namespace = namespace();

        String event = "{\"timestamp\":" + ((System.currentTimeMillis() / 1000)) + ", \"sensor_uuid\":7, \"type\":\"ips\", \"sensor_name\":\"rbips\", \"sensor_ip\":\"90.1.44.3\"," +
                " \"domain_name\":\"IPS\", \"group_name\":\"default\", \"group_id\":8, \"sig_generator\":1, \"sig_id\":25521," +
                " \"rev\":3, \"priority\":\"high\", \"classification\":\"" + classification[classificationInt] + "\"," +
                " \"action\":\"alert\", \"msg\":\"" + msg[msgInt] + "\"," +
                " \"payload\":\"474554202f737469636b6572732f6e6f74696669636174696f6e732e6a736f6e20485454502f312e31da557365722d4167656e743a2044616c76696b2f312e362e3020284c6" +
                "96e75783b20553b20416e64726f696420342e342e333b20485443363530304c5657204275696c642f4b545538344c29da486f73743a20636f6e74656e742e63646e2e76696265722e63" +
                "6f6dda436f6e6e656374696f6e3a204b6565702d416c697665da4163636570742d456e636f64696e673a20677a6970dada\", \"l4_proto\":6, " +
                "\"src\":\"" + getIP() + "\", \"src_net\":\"0.0.0.0/0\", \"src_net_name\":\"0.0.0.0/0\", \"src_as\":8121," +
                " \"src_as_name\":\"TCH Network Services\", \"dst\":\"" + getIP() + "\", \"dst_net\":\"0.0.0.0/0\"," +
                " \"dst_net_name\":\"0.0.0.0/0\", \"dst_as\":2914, \"dst_as_name\":\"NTT America, Inc.\"," +
                " \"src_port\":92, \"dst_port\":80, \"ethsrc\":\"" + client_mac + "\", " +
                "\"ethdst\":\"" + getMac() + "\", \"ethlength\":264, \"sha256\":\"" + getSha256() + "\", " +
                "\"file_size\":372373, \"file_uri\": \"http://www.test.com/asd/test.gz\", \"file_hostname\":\"test.com\", " +
                "\"sensor_name\":\"sensor_" + namespace + "_" + tiers.get(namespace) + "\" ," +
                "\"namespace_uuid\":\"" + namespace + "\", \"tier\":\"" + tiers.get(namespace) + "\", \"sensor_uuid\":" + getSensorId(namespace) + ", " +
                " \"ethlength_range\":\"(256-512]\", \"tcpflags\":\"***AP***\"," +
                " \"tcpseq\":432143985, \"tcpack\":3505747298, \"tcplen\":32, " +
                "\"tcpwindow\":229, \"ttl\":" + new Random().nextInt(120) + ", \"tos\":0, \"id\":31461, " +
                "\"dgmlen\":250, \"iplen\":256000, \"iplen_range\":\"[131072-262144)\"," +
                " \"src_country\":\"United States\", \"dst_country\":\"United States\", \"ControllerSDN\":\"OFL\"," +
                " \"src_country_code\":\"US\", \"dst_country_code\":\"US\", \"ethsrc_vendor\":\"Cisco\", \"ethdst_vendor\":\"Cisco\", \"sha256\":\"" + sha256 + "\" }";

        return new KeyedMessage<String, String>("rb_event", client_mac, event);
    }

    public KeyedMessage getMalware(String sha256) {
        String client_mac = getMac();
        String namespace = namespace();

        String event = "{\"timestamp\":" + ((System.currentTimeMillis() / 1000)) + ", \"sha256\":\"" + sha256 + "\", " +
                "\"sensor_name\":\"sensor_" + namespace + "_" + tiers.get(namespace) + "\" , \"malware_name\": \"" + sha256MalwareNames.get(sha256) + "\", " +
                "\"namespace_uuid\":\"" + namespace + "\", \"sensor_uuid\":" + getSensorId(namespace) + ", \"score\":50}";

        return new KeyedMessage<String, String>("rb_malware", client_mac, event);
    }

    public KeyedMessage getMail() {
        String client_mac = getMac();
        String namespace = namespace();
        String sha256 = getSha256();

        String event = "{\"timestamp\":" + ((System.currentTimeMillis() / 1000)) + ", \"sha256\":\"" + sha256 + "\", " +
                "\"sensor_name\":\"sensor_" + namespace + "_" + tiers.get(namespace) + "\" , \"malware_name\": \"" + sha256MalwareNames.get(sha256) + "\", " +
                "\"namespace_uuid\":\"" + namespace + "\", \"sensor_uuid\":" + getSensorId(namespace) + "}";

        return new KeyedMessage<String, String>("rb_mail", client_mac, event);
    }

    public String getUser() {
        String[] user = {"andres", "pepe", "jaime", "pablo", "carlos", "jota", "clara", "raquel", "eu", "carlosR", "angel"};
        int users = new Random().nextInt(user.length);
        return user[users];
    }

    public static String sha256(String base) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(base.getBytes("UTF-8"));
            StringBuffer hexString = new StringBuffer();

            for (int i = 0; i < hash.length; i++) {
                String hex = Integer.toHexString(0xff & hash[i]);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }

            return hexString.toString();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public String getSha256() {
        String random = Integer.valueOf(new Random().nextInt(10000000)).toString();
        return sha256(random);
    }

    public String getGender() {
        String[] gender = {"male", "female"};
        int genders = new Random().nextInt(gender.length);
        return gender[genders];
    }

    public String getLang() {
        String[] lang = {"es", "us", "en", "rs", "fr", "xs", "lm", "cy", "pe", "ab", "er"};
        int langs = new Random().nextInt(lang.length);
        return lang[langs];
    }

    public String getType() {
        String[] type = {"twitter", "facebook",};
        int types = new Random().nextInt(type.length);
        return type[types];
    }

    public String getDevice() {
        String[] device = {"Iphone", "Acer", "MacBookPro", "iMac", "LG"};
        int devices = new Random().nextInt(device.length);
        return device[devices];
    }

    public KeyedMessage getSocial() {
        double lat = 37.40;
        double lon = 26.97;

        double random = randomX.nextInt(100);
        double random2 = randomX.nextInt(100);
        double randomOk = random / 100;
        double random1k = random2 / 100;

        int id = 136447 * randomX.nextInt(100000);

        String type = getType();
        String social = "{\"sensor_name\": \"rb_social\"," +
                " \"type\":\"" + type + "\"," +
                " \"username\":\"" + getUser() + "\", \"avatar\":\"http://a2.twimg.com/profile_images/1302306721/twitterpic_normal.jpg\"," +
                " \"link\":\"http://" + type + ".com/statuses/" + id + "\"," +
                " \"timestamp\": " + (System.currentTimeMillis() / 1000) + ", \"msg\": \"(content)\", \"client_latlong\":\"" + (lat + randomOk) + "," + (lon - random1k) + "\"," +
                " \"device\": \"" + getDevice() + "\", \"gender\":\"" + getGender() + "\", \"sentiment\": " + randomX.nextInt(10) + ", \"language\": \" " + getLang() + "\"}";

        return new KeyedMessage<String, String>("rb_social", social);
    }
}

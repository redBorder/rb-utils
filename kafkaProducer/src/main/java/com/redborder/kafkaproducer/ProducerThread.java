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
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 7/11/14.
 */
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



    public ProducerThread(String zookeeper, String topic, String brokerList, Integer events, Integer id, boolean enrich) {
        this.zookeeper = zookeeper;
        this.topics = topic;
        this._brokerList = brokerList;
        this.events = events;
        this.id = id;
        this.enrich = enrich;
        tiers.put("deployment_a", "gold");
        tiers.put("deployment_b", "silver");
        tiers.put("deployment_c", "unknown");
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

            if (topicsList.contains("rb_social")) {
                producer.send(getSocial());
            }
            if (topicsList.contains("rb_event")) {
                producer.send(getEvent());
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


    public static String getMac() {
        String[] macs = {"54:26:96:d7:62:91", "b4:52:7e:8d:24:0f",
                "88:32:9b:4e:71:36", "b4:52:7e:88:ca:53",
                "b4:52:7e:8d:23:46", "b4:52:7e:7d:5d:8c",
                "98:d6:f7:67:36:cf", "88:11:77:aa:bb:22",
                "99:aa:bb:cc:11:00", "00:a3:b5:77:1a:3c"};

        int idx = new Random().nextInt(macs.length);
        return macs[idx];

    }

    public static String getAPmac() {
        String[] macs = {"11:11:11:11:11:11", "22:22:22:22:22:22",
                "33:33:33:33:33:33"};

        int idx = new Random().nextInt(macs.length);
        return macs[idx];

    }

    public static String getIP() {
        String[] ip = {"192.168.2.3", "80.2.11.4", "90.1.44.3",
                "98.11.22.55"};

        int ips = new Random().nextInt(ip.length);

        return ip[ips];
    }

    public static String getSSID() {
        String[] ip = {"ssid_A", "ssid_B", "ssid_C"};

        int ips = new Random().nextInt(ip.length);

        return ip[ips];
    }

    public static String deployment() {
        String[] ip = {"deployment_a", "deployment_b", "deployment_c"};

        int ips = new Random().nextInt(ip.length);

        return ip[ips];
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

    public KeyedMessage getFlow() {

        String[] status = {"ASSOCIATED", "PROBING", "UNKNOWN"};
        int statusInt = randomX.nextInt(status.length);

        String[] zonas = {"Zone X", "Zone C", "Zone A", "Zone B",
                "Zone Y"};
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
        String deployment = deployment();


        if (enrich) {

            flow = "{\"client_latlong\":\"" + (lat + randomOk) + "," + (lon - random1k) + "\"," +
                    "\"dst_country_code\":\"US\",\"dot11_status\":\"" + status[statusInt] + "\"," +
                    "\"bytes\":" + new Random().nextInt(3000) + ",\"src_net_name\":\"0.0.0.0/0\",\"flow_sampler_id\":0," +
                    "\"direction\":\"ingress\",\"wireless_station\":\"" + getAPmac() + "\"," +
                    "\"biflow_direction\":\"initiator\",\"pkts\":" + randomX.nextInt(500) + ",\"dst\":\"" + getIP() + "\"," +
                    "\"type\":\"NetFlowv10\",\"client_campus\":\"" + zonas[zonaInt] + " campus" + "\"," +
                    "\"client_building\":\"" + zonas[zonaInt] + " building" + "\",\"timestamp\":" + ((System.currentTimeMillis() / 1000)) + "," +
                    "\"client_mac\":\"" + client_mac + "\",\"wireless_id\":\"" + getSSID() + "\"," +
                    "\"flow_end_reason\":\"idle timeout\",\"src_net\":\"0.0.0.0/0\"," +
                    "\"client_rssi_num\":" + (-randomX.nextInt(80)) + ",\"engine_id_name\":\"IANA-L4\"," +
                    "\"src\":\"" + getIP() + "\",\"application_id\":\"" + randomX.nextInt(10) + ":" + randomX.nextInt(100) + "\"," +
                    "\"sensor_ip\":\"90.1.44.3\"," +
                    "\"application_id_name\":\"" + apps[zonaInt] + "\",\"dst_net\":\"0.0.0.0/0\"," +
                    "\"l4_proto\":" + randomX.nextInt(10) + ",\"ip_protocol_version\":4,\"dst_net_name\":\"0.0.0.0/0\"," +
                    "\"sensor_name\":\"ISG\",\"src_country_code\":\"US\"," +
                    "\"client_floor\":\"" + zonas[zonaInt] + " floor" + "\",\"engine_id\":" + randomX.nextInt(20) +
                    ",\"client_mac_vendor\":\"SAMSUNG ELECTRO-MECHANICS\", \"first_switched\": " + ((System.currentTimeMillis() / 1000) - (2 * 60)) + ", \"deployment_id\": \"" + deployment + "\", \"tier\":\"" + tiers.get(deployment) +"\"}";
        }else {

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
                    "\"sensor_name\":\"TESTING\"," +
                    "\"engine_id\":" + randomX.nextInt(20) +
                    ", \"first_switched\": " + ((System.currentTimeMillis() / 1000) - (2 * 60)) + ", \"deployment_id\": \"" + deployment + "\", \"tier\":\"" + tiers.get(deployment) +"\"}";
        }

        return new KeyedMessage<String, String>("rb_flow", client_mac, flow);
    }

    public static KeyedMessage getEvent() {

        String[] classification = {"Potential Corporate Privacy Violation", "Sensitive Data was Transmitted Across the Network", "Potentially Bad Traffic",
                "Unknown Traffic"};

        String[] msg = {"SERVER-IIS Microsoft IIS 7.5 client verify null pointer attempt", "OS-MOBILE Android User-Agent detected", "sensitive_data: sensitive data - U.S. social security numbers without dashes"
                , "http_inspect: OVERSIZE REQUEST-URI DIRECTORY", "OS-WINDOWS Microsoft Windows UPnP Location overflow attempt", "ssp_ssl: Invalid Client HELLO after Server HELLO Detected"};

        int classificationInt = new Random().nextInt(classification.length);
        int msgInt = new Random().nextInt(msg.length);
        String client_mac = getMac();

        String event = "{\"timestamp\":" + ((System.currentTimeMillis() / 1000)) + ", \"sensor_id\":7, \"type\":\"ips\", \"sensor_name\":\"rbips\", \"sensor_ip\":\"90.1.44.3\"," +
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
                "\"ethdst\":\"" + getMac() + "\", \"ethlength\":264," +
                " \"ethlength_range\":\"(256-512]\", \"tcpflags\":\"***AP***\"," +
                " \"tcpseq\":432143985, \"tcpack\":3505747298, \"tcplen\":32, " +
                "\"tcpwindow\":229, \"ttl\":" + new Random().nextInt(120) + ", \"tos\":0, \"id\":31461, " +
                "\"dgmlen\":250, \"iplen\":256000, \"iplen_range\":\"[131072-262144)\"," +
                " \"src_country\":\"United States\", \"dst_country\":\"United States\"," +
                " \"src_country_code\":\"US\", \"dst_country_code\":\"US\", \"ethsrc_vendor\":\"Cisco\", \"ethdst_vendor\":\"Cisco\"}";

        return new KeyedMessage<String, String>("rb_event", client_mac, event);
    }

    public String getUser(){
        String[] user = {"andres", "pepe", "jaime", "pablo", "carlos", "jota", "clara", "raquel", "eu", "carlosR", "angel"};
        int users = new Random().nextInt(user.length);
        return user[users];
    }

    public String getGender(){
        String[] gender = {"male", "female"};
        int genders = new Random().nextInt(gender.length);
        return gender[genders];
    }

    public String getLang(){
        String[] lang = {"es", "us", "en", "rs", "fr", "xs", "lm", "cy", "pe", "ab", "er"};
        int langs = new Random().nextInt(lang.length);
        return lang[langs];
    }

    public String getType(){
        String[] type = {"twitter", "facebook",};
        int types = new Random().nextInt(type.length);
        return type[types];
    }

    public String getDevice(){
        String[] device = {"Iphone", "Acer", "MacBookPro", "iMac", "LG"};
        int devices = new Random().nextInt(device.length);
        return device[devices];
    }

    public  KeyedMessage getSocial(){

        double lat = 37.40;
        double lon = 26.97;

        double random = randomX.nextInt(100);
        double random2 = randomX.nextInt(100);
        double randomOk = random / 100;
        double random1k = random2 / 100;

        int id = 136447 * randomX.nextInt(100000);




        String type = getType();
        String social = "{\"sensor_name\": \"rb_social\"," +
                " \"type\":\""+type+"\"," +
                " \"username\":\""+getUser()+"\", \"avatar\":\"http://a2.twimg.com/profile_images/1302306721/twitterpic_normal.jpg\"," +
                " \"link\":\"http://"+type+".com/statuses/"+id+"\"," +
                " \"timestamp\": "+ (System.currentTimeMillis() / 1000)  +", \"msg\": \"(content)\", \"client_latlong\":\""+ (lat + randomOk) + "," + (lon - random1k) +"\"," +
                " \"device\": \""+getDevice()+"\", \"gender\":\""+getGender()+"\", \"sentiment\": "+randomX.nextInt(10)+", \"language\": \" " + getLang() + "\"}";

        return new KeyedMessage<String, String>("rb_social", social);
    }
}

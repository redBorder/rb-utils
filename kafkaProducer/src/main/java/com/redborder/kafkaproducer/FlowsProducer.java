package com.redborder.kafkaproducer;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

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

import org.apache.commons.cli.*;


public class FlowsProducer {

    static Producer<String, String> producer;

    static String _brokerList = new String();

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

    public static KeyedMessage getLocation() {


        String[] status = {"ASSOCIATED", "PROBING", "UNKNOWN"};
        int statusInt = new Random().nextInt(status.length);

        String[] zonas = {"Zone X", "Zone C", "Zone A", "Zone B",
                "Zone Y"};
        int zonaInt = new Random().nextInt(zonas.length);


        double lat = 37.40;
        double lon = 26.97;

        double random = new Random().nextInt(100);
        double random2 = new Random().nextInt(100);
        double randomOk = random / 100;
        double random1k = random2 / 100;

        int r = new Random().nextInt(100);

        DateTime date = new DateTime();

        String loc = "{\"StreamingNotification\":{\"subscriptionName\":\"ENEO_STREAM_JSON_TCP\",\"entity\":\"WIRELESS_CLIENTS\","
                + "\"deviceId\":\"" + getMac() + "\",\"mseUdi\":\"AIR-MSE-3355-K9:V03:KQ4V9TX\",\"floorRefId\":0,"
                + "\"location\":{\"macAddress\":\"" + getMac() + "\",\"mapInfo\""
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

        return new KeyedMessage<String, String>("rb_loc", loc);
    }

    public static KeyedMessage getFlow() {

        String flow = "{\"type\":\"NetFlowv9\",\"client_mac\":\"" + getMac() + "\","
                + "\"src\":\"" + getIP() + "\",\"src_net\":\"0.0.0.0/0\","
                + "\"src_net_name\":\"0.0.0.0/0\",\"application_id\":\"13:453\","
                + "\"application_id_name\":\"ssl\",\"engine_id\":13,"
                + "\"engine_id_name\":\"PANA-L7\","
                + "\"direction\":\"egress\","
                + "\"sensor_ip\":\"192.168.101.66\",\"sensor_name\":\"TEST\","
                + "\"timestamp\":" + (System.currentTimeMillis() / 1000) + ",\"bytes\":" + new Random().nextInt(3000) + ",\"pkts\":" + new Random().nextInt(300) + "}";

        return new KeyedMessage<String, String>("rb_flow", flow);
    }


    public static void main(String[] args) throws InterruptedException {


        CommandLine cmdLine = null;
        List<String> topicsList = null;
        boolean run = true;

        int events = 100;
        long time = 0;

        Options options = new Options();

        options.addOption("zk", true, "Zookeeper servers.");
        options.addOption("topics", true, "Topics [rb_flow, rb_loc].");
        options.addOption("s", true, "Time to sleep (milliseconds) [Default: 0].");
        options.addOption("h", "help", false, "Print help.");


        CommandLineParser parser = new BasicParser();
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if (cmdLine.hasOption("h")) {
            new HelpFormatter().printHelp(FlowsProducer.class.getCanonicalName(), options);
            return;
        }


        if (cmdLine.hasOption("s")) {
            events = Integer.valueOf(cmdLine.getOptionValue("s"));
            if (events != 0)
                time = 1000 / events;
            else
                time = 0;
        }


        if (!(cmdLine.hasOption("zk") && cmdLine.hasOption("topics"))) {
            System.out.println("You must specify zk and topics");
            new HelpFormatter().printHelp(FlowsProducer.class.getCanonicalName(), options);
            return;
        }

        configProducer(cmdLine.getOptionValue("zk"));
        String topics = cmdLine.getOptionValue("topics");

        if (!(topics.contains("rb_flow") || topics.contains("rb_loc"))) {
            System.out.println("Available topics: rb_flow   rb_loc");
            return;
        }

        if (topics.contains(","))
            topicsList = Arrays.asList(topics.split(","));
        else if (topics.contains(":"))
            topicsList = Arrays.asList(topics.split(":"));
        else {
            topicsList = new ArrayList<String>();
            topicsList.add(topics);
        }


        System.out.println("Producing to: " + _brokerList);
        while (run) {

            if (topicsList.contains("rb_flow")) {
                producer.send(getFlow());
            }
            if (topicsList.contains("rb_loc")) {
                producer.send(getLocation());
            }

            Thread.sleep(time);
        }

        new HelpFormatter().printHelp(FlowsProducer.class.getCanonicalName(), options);
        return;
    }


    public static void configProducer(String zookeeper) {


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

        Properties props = new Properties();
        props.put("metadata.broker.list", _brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        props.put("message.send.max.retries", "60");
        props.put("retry.backoff.ms", "1000");
        props.put("producer.type", "async");
        props.put("queue.buffering.max.messages", "10000");
        props.put("queue.buffering.max.ms", "500");

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);

        client.close();
    }
}

package com.redborder.kafkaproducer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.cli.*;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;


public class FlowsProducer {

    static Producer<String, String> producer;
    static String _brokerList = new String("");


    public static void main(String[] args) throws InterruptedException {


        final List<ProducerThread> threads = new ArrayList<ProducerThread>();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {

                for (ProducerThread thread : threads) {
                    thread.terminate();
                }

                System.out.println("Shutdown!");
            }
        });

        CommandLine cmdLine = null;

        Integer events = null;

        Options options = new Options();

        Integer partitions = 1;

        options.addOption("zk", true, "Zookeeper servers.");
        options.addOption("topics", true, "Topics [rb_flow, rb_loc].");
        options.addOption("s", true, "Flows per seconds per thread.");
        options.addOption("b", true, "Brokers to send.");
        options.addOption("p", true, "Number of threads.");
        options.addOption("e", "enrichment", true, "Active enrichment.");
        options.addOption("i", true, "Input File.");
        options.addOption("l", false, "Input File Loop.");
        options.addOption("r", true, "Repeat X times.");
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

        if (!cmdLine.hasOption("i")) {
            if (cmdLine.hasOption("s")) {
                events = Integer.valueOf(cmdLine.getOptionValue("s"));

            }


            if (!cmdLine.hasOption("topics")) {
                System.out.println("You must specify topics");
                new HelpFormatter().printHelp(FlowsProducer.class.getCanonicalName(), options);
                return;
            }


            String topics = cmdLine.getOptionValue("topics");

            if (!(topics.contains("rb_flow") || topics.contains("rb_loc") || topics.contains("rb_event") || topics.contains("rb_social"))) {
                System.out.println("Available topics: rb_flow   rb_loc   rb_event");
                return;
            }

            if (cmdLine.hasOption("p")) {
                partitions = Integer.valueOf(cmdLine.getOptionValue("p"));
            }

            boolean enrich = true;

            if (cmdLine.hasOption("e")) {
                enrich = Boolean.valueOf(cmdLine.getOptionValue("e"));
            }

            for (int i = 0; i < partitions; i++) {

                if (!cmdLine.hasOption("b")) {
                    threads.add(new ProducerThread(cmdLine.getOptionValue("zk"), topics, "", events, i, enrich));
                } else {
                    threads.add(new ProducerThread(cmdLine.getOptionValue("zk"), topics, cmdLine.getOptionValue("b"), events, i, enrich));

                }

            }

            for (ProducerThread thread : threads) {
                thread.start();
            }

        } else {
            boolean loop = true;


            ObjectMapper mapper = new ObjectMapper();

            Long delta = 0L;


            if (_brokerList.equals("")) {
                configProducer(cmdLine.getOptionValue("zk"), false);
            } else {
                configProducer(cmdLine.getOptionValue("zk"), true);
            }

            long millis = 1;

            if (cmdLine.getOptionValue("topics").contains("rb_loc")) {
                millis = 1000;
            } else if (cmdLine.getOptionValue("topics").contains("rb_flow")) {
                millis = 1;
            }

            Integer repeat = 1;
            if (cmdLine.hasOption("r")) {
                repeat = Integer.valueOf(cmdLine.getOptionValue("r"));

            }

            long produces = 0L;

            while (loop) {
                try {
                    BufferedReader br = new BufferedReader(new FileReader(cmdLine.getOptionValue("i")));
                    String sCurrentLine;

                    while ((sCurrentLine = br.readLine()) != null) {
                        try {
                            Map<String, Object> event = mapper.readValue(sCurrentLine, Map.class);
                            if (delta == 0) {
                                Long remoteTimestamp;

                                if (cmdLine.getOptionValue("topics").contains("rb_loc")) {
                                    List<Map<String, Object>> list = (ArrayList) event.get("notifications");
                                    remoteTimestamp = Long.valueOf(String.valueOf(list.get(0).get("timestamp")));
                                } else {
                                    remoteTimestamp = Long.valueOf(String.valueOf(event.get("timestamp")));
                                }

                                Long localTimestamp = System.currentTimeMillis() / 1000 * millis;
                                delta = localTimestamp - remoteTimestamp;

                                if (cmdLine.getOptionValue("topics").contains("rb_loc")) {
                                    List<Map<String, Object>> list = (ArrayList) event.get("notifications");
                                    Map<String, Object> content = list.remove(0);
                                    content.put("timestamp", localTimestamp);
                                    content.put("time_original", remoteTimestamp);
                                    list.add(content);
                                    event.put("notifications", list);

                                } else {
                                    event.put("time_original", remoteTimestamp);
                                    event.put("timestamp", localTimestamp);
                                }
                                if (event.containsKey("first_switched"))
                                    event.put("first_switched", Long.valueOf(String.valueOf(event.get("first_switched"))) + delta);
                            } else {
                                Long remoteTimestamp;

                                if (cmdLine.getOptionValue("topics").contains("rb_loc")) {
                                    List<Map<String, Object>> list = (ArrayList) event.get("notifications");
                                    remoteTimestamp = Long.valueOf(String.valueOf(list.get(0).get("timestamp")));
                                } else {
                                    remoteTimestamp = Long.valueOf(String.valueOf(event.get("timestamp")));
                                }

                                Long toSleep = 0L;

                                if (remoteTimestamp + delta > System.currentTimeMillis() / 1000 * millis) {
                                    produces++;
                                    toSleep = (remoteTimestamp + delta - System.currentTimeMillis() / 1000 * millis) * 1000;
                                    System.out.printf("%-10s  %-17s  %-10s \n", DateTime.now().toString(), " Produced: " + produces, " Sleep: " + toSleep / 1000 + " secs");
                                    produces = 0L;
                                    Thread.sleep(toSleep);
                                } else {
                                    produces++;
                                }

                                if (cmdLine.getOptionValue("topics").contains("rb_loc")) {
                                    List<Map<String, Object>> list = (ArrayList) event.get("notifications");
                                    Map<String, Object> content = list.remove(0);
                                    content.put("timestamp", System.currentTimeMillis() / 1000 * millis);
                                    content.put("time_original", remoteTimestamp);
                                    list.add(content);
                                    event.put("notifications", list);

                                } else {
                                    event.put("time_original", remoteTimestamp);
                                    event.put("timestamp", System.currentTimeMillis() / 1000 * millis);
                                }

                                if (event.containsKey("first_switched"))
                                    event.put("first_switched", Long.valueOf(String.valueOf(event.get("first_switched"))) + delta + toSleep);
                            }
                            String json = mapper.writeValueAsString(event);
                            Integer times = 0;

                            while (times < repeat) {
                                producer.send(new KeyedMessage<String, String>(cmdLine.getOptionValue("topics"), json));
                                times++;
                            }

                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    delta = 0L;

                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                if (cmdLine.hasOption("l")) {
                    loop = true;
                } else {
                    loop = false;
                }
            }

            producer.close();
        }
    }

    public static void configProducer(String zookeeper, boolean broker) {

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

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);

    }

}

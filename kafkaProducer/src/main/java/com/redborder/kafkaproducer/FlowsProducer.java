package com.redborder.kafkaproducer;

import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.List;


public class FlowsProducer {


    public static void main(String[] args) throws InterruptedException {


        final List<ProducerThread> threads= new ArrayList<ProducerThread>();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {

                for(ProducerThread thread : threads){
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

        }


        if (!cmdLine.hasOption("topics")) {
            System.out.println("You must specify topics");
            new HelpFormatter().printHelp(FlowsProducer.class.getCanonicalName(), options);
            return;
        }


        String topics = cmdLine.getOptionValue("topics");

        if (!(topics.contains("rb_flow") || topics.contains("rb_loc") || topics.contains("rb_event"))) {
            System.out.println("Available topics: rb_flow   rb_loc   rb_event");
            return;
        }

        if(cmdLine.hasOption("p")){
            partitions=Integer.valueOf(cmdLine.getOptionValue("p"));
        }

        boolean enrich = true;

        if(cmdLine.hasOption("e")){
            enrich=Boolean.valueOf(cmdLine.getOptionValue("e"));
        }

        for(int i=0; i<partitions;i++) {

            if (!cmdLine.hasOption("b")) {
                threads.add(new ProducerThread(cmdLine.getOptionValue("zk"), topics, "", events, i, enrich));
            } else {
                threads.add(new ProducerThread(cmdLine.getOptionValue("zk"), topics, cmdLine.getOptionValue("b"), events, i, enrich));

            }

        }

        for(ProducerThread thread: threads){
            thread.start();
        }

    }
}

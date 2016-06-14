package net.redborder.utils.storm;

import backtype.storm.generated.Nimbus.Client;

public class NimbusConfiguration {

    public void printNimbusStats(String ui_node) {
        try {
            ThriftClient thriftClient = new ThriftClient(ui_node);
            Client client = thriftClient.getClient();
            String nimbusConiguration = client.getNimbusConf();
            System.out.println("************************************************************************");
            System.out.println("                             Nimbus Configuration                       ");
            System.out.println("************************************************************************");
            System.out.println(nimbusConiguration);
        } catch (Exception exception) {
            throw new RuntimeException("Error occurred while fetching the Nimbus statistics : ");
        }
    }
}

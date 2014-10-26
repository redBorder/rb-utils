package net.redborder.utils.storm;

import backtype.storm.generated.*;
import backtype.storm.generated.Nimbus.Client;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;

public class SpoutStatistics {

    private static final String DEFAULT = "default";
    private static final String ALL_TIME = ":all-time";

    public void printSpoutStatistics(String ui_node) {
        try {
            ThriftClient thriftClient = new ThriftClient(ui_node);
            // Get the nimbus thrift client
            Client client = thriftClient.getClient();
            // Get the information of given topology
            String topologyId = topologyID(ui_node);
            if(topologyId!=null) {
                TopologyInfo topologyInfo = client.getTopologyInfo(topologyId);
                Iterator<ExecutorSummary> executorSummaryIterator = topologyInfo
                        .get_executors_iterator();
                while (executorSummaryIterator.hasNext()) {
                    ExecutorSummary executorSummary = executorSummaryIterator.next();
                    ExecutorStats executorStats = executorSummary.get_stats();
                    if (executorStats != null) {
                        ExecutorSpecificStats executorSpecificStats = executorStats.get_specific();
                        String componentId = executorSummary.get_component_id();
                        //
                        if (executorSpecificStats.is_set_spout()) {
                            SpoutStats spoutStats = executorSpecificStats.get_spout();
                            System.out.println("******************************************************************");
                            System.out.println("Component ID of Spout:- " + componentId);
                            System.out.println("Transferred:- "
                                    + getAllTimeStat(executorStats.get_transferred(), ALL_TIME));
                            System.out.println("Total tuples emitted:- "
                                    + getAllTimeStat(executorStats.get_emitted(), ALL_TIME));
                            System.out.println("Acked: "
                                    + getAllTimeStat(spoutStats.get_acked(),
                                    ALL_TIME));
                            System.out.println("Failed: "
                                    + getAllTimeStat(spoutStats.get_failed(),
                                    ALL_TIME));
                            System.out.println("******************************************************************");
                        }
                    }
                }
            }else {
                System.out.println("ID not valid!");
            }
        } catch (Exception exception) {
            throw new RuntimeException("Error occurred while fetching the spout information : " + exception);
        }
    }

    private static Long getAllTimeStat(Map<String, Map<String, Long>> map,
                                       String statName) {
        if (map != null) {
            Long statValue = null;
            Map<String, Long> tempMap = map.get(statName);
            statValue = tempMap.get(DEFAULT);
            return statValue;
        }
        return 0L;
    }

    private String topologyID(String ui_node) {

        Integer id = 0;
        Map<String, String> topologyID = new HashMap<String, String>();
        String idStr = null;

        try {
            ThriftClient thriftClient = new ThriftClient(ui_node);
            // Get the thrift client
            Client client = thriftClient.getClient();
            // Get the cluster info
            ClusterSummary clusterSummary = client.getClusterInfo();
            // Get the interator over TopologySummary class
            Iterator<TopologySummary> topologiesIterator = clusterSummary.get_topologies_iterator();
            while (topologiesIterator.hasNext()) {
                TopologySummary topologySummary = topologiesIterator.next();
                System.out.println("ID[" + id + "]: " + topologySummary.get_id());
                topologyID.put(id.toString(), topologySummary.get_id());
                id++;
            }

            System.out.print("Choose some topology id: ");
            Scanner scanner = new Scanner(System.in);
            idStr = scanner.next();

        } catch (Exception e) {
            throw new RuntimeException("Error occurred while getting cluster info : ");
        }

        if (idStr != null)
            return topologyID.get(idStr);
        else
            return idStr;
    }

}

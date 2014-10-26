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
            System.out.println("************************************************************************");
            System.out.println("                              Spouts                                    ");
            System.out.println("************************************************************************");
            for (TopologySummary topology : client.getClusterInfo().get_topologies()) {
                TopologyInfo topologyInfo = client.getTopologyInfo(topology.get_id());
                Iterator<ExecutorSummary> executorSummaryIterator = topologyInfo
                        .get_executors_iterator();

                System.out.println("  TOPOLOGY: " + topologyInfo.get_name());
                while (executorSummaryIterator.hasNext()) {
                    ExecutorSummary executorSummary = executorSummaryIterator.next();
                    ExecutorStats executorStats = executorSummary.get_stats();
                    if (executorStats != null) {
                        ExecutorSpecificStats executorSpecificStats = executorStats.get_specific();
                        String componentId = executorSummary.get_component_id();
                        //
                        if (executorSpecificStats.is_set_spout()) {
                            SpoutStats spoutStats = executorSpecificStats.get_spout();
                            System.out.println("    Component ID of Spout:- " + componentId);
                            System.out.println("      - Transferred:- "
                                    + getAllTimeStat(executorStats.get_transferred(), ALL_TIME));
                            System.out.println("      - Total tuples emitted:- "
                                    + getAllTimeStat(executorStats.get_emitted(), ALL_TIME));
                            System.out.println("      - Acked: "
                                    + getAllTimeStat(spoutStats.get_acked(),
                                    ALL_TIME));
                            System.out.println("      - Failed: "
                                    + getAllTimeStat(spoutStats.get_failed(),
                                    ALL_TIME));
                        }
                    }
                }
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
}

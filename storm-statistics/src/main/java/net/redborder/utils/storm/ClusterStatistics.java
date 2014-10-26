package net.redborder.utils.storm;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologySummary;

public class ClusterStatistics {

    public void printClusterStatistics(String ui_node) {
        try {
            ThriftClient thriftClient = new ThriftClient(ui_node);
            Client client = thriftClient.getClient();
            ClusterSummary clusterInfo = client.getClusterInfo();
            System.out.println("************************************************************************");
            System.out.println("                            Storm cluster info                         ");
            System.out.println("************************************************************************");
            System.out.println(" Nimbus uptime: " +clusterInfo.get_nimbus_uptime_secs() + " seconds. \n");

            for(SupervisorSummary supervisor : clusterInfo.get_supervisors()) {
                System.out.println(" Supervisor:  ");
                System.out.println("  - Host: "+ supervisor.get_host());
                System.out.println("  - Total workers: "+ supervisor.get_num_workers());
                System.out.println("  - Used workers: "+ supervisor.get_num_used_workers());
                System.out.println("  - Uptime seconds: "+ supervisor.get_uptime_secs());
            }
            System.out.println("");
            for(TopologySummary topology : clusterInfo.get_topologies()) {
                System.out.println(" Topology:  ");
                System.out.println("  - Name: "+ topology.get_name());
                System.out.println("  - Status: "+ topology.get_status() );
                System.out.println("  - Tasks: "+ topology.get_num_tasks());
                System.out.println("  - Executors: "+ topology.get_num_executors());
                System.out.println("  - Workers: "+ topology.get_num_workers() );
                System.out.println("  - Uptime seconds: "+ topology.get_uptime_secs());

            }
        } catch (Exception exception) {
            throw new RuntimeException("Error occurred while fetching the Nimbus statistics : ");
        }
    }
}
package net.redborder.utils.storm;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.SupervisorSummary;

import java.util.Iterator;

public class SupervisorStatistics {

    public void printSupervisorStatistics(String ui_node) {
        try {
            ThriftClient thriftClient = new ThriftClient(ui_node);
            Client client = thriftClient.getClient();
            // Get the cluster information.
            ClusterSummary clusterSummary = client.getClusterInfo();
            // Get the SupervisorSummary interator
            Iterator<SupervisorSummary> supervisorsIterator = clusterSummary.get_supervisors_iterator();
            System.out.println("************************************************************************");
            System.out.println("                                 Supervisors                            ");
            System.out.println("************************************************************************");
            while (supervisorsIterator.hasNext()) {
                // Print the information of supervisor node
                SupervisorSummary supervisorSummary = (SupervisorSummary) supervisorsIterator.next();
                System.out.println(" Supervisor:  ");
                System.out.println("     - Host: " + supervisorSummary.get_host());
                System.out.println("     - Number of used workers : " + supervisorSummary.get_num_used_workers());
                System.out.println("     - Number of workers : " + supervisorSummary.get_num_workers());
                System.out.println("     - Supervisor ID : " + supervisorSummary.get_supervisor_id());
                System.out.println("     - Supervisor uptime in seconds : " + supervisorSummary.get_uptime_secs() +"\n");
            }


        } catch (Exception e) {
            throw new RuntimeException("Error occurred while getting cluster info : ");
        }
    }
}

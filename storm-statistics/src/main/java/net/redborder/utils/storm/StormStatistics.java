package net.redborder.utils.storm;


/**
 * Created by andresgomez on 25/10/14.
 */
public class StormStatistics {

    public static void main(String[] args) {

        String str = args[0];

        if(str.equals("nimbusConf")){
            new NimbusConfiguration().printNimbusStats(args[1]);
        }else if(str.equals("supervisor")){
            new SupervisorStatistics().printSupervisorStatistics(args[1]);
        }else if(str.equals("topology")){
            new TopologyStatistics().printTopologyStatistics(args[1]);
        }else if(str.equals("spout")){
            new SpoutStatistics().printSpoutStatistics(args[1]);
        }else if(str.equals("bolt")){
            new BoltStatistics().printBoltStatistics(args[1]);
        }else if(str.equals("cluster")){
            new ClusterStatistics().printClusterStatistics(args[1]);
        }

    }

}

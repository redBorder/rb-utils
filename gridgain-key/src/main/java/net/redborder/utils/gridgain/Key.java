package net.redborder.utils.gridgain;

import com.amazonaws.auth.BasicAWSCredentials;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.gridgain.grid.Grid;
import org.gridgain.grid.GridConfiguration;
import org.gridgain.grid.GridException;
import org.gridgain.grid.GridGain;
import org.gridgain.grid.cache.GridCache;
import org.gridgain.grid.cache.GridCacheConfiguration;
import org.gridgain.grid.cache.GridCacheDistributionMode;
import org.gridgain.grid.cache.GridCacheMode;
import org.gridgain.grid.spi.discovery.tcp.GridTcpDiscoverySpi;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.s3.GridTcpDiscoveryS3IpFinder;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.GridTcpDiscoveryVmIpFinder;
import org.ho.yaml.Yaml;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by andresgomez on 07/07/14.
 */
public class Key {

    private final static String CONFIG_FILE_PATH = "/opt/rb/etc/redBorder-BI/config.yml";

    public static void main(String[] args) throws FileNotFoundException {


        Map<String, Object> configFile = (Map<String, Object>) Yaml.load(new File(CONFIG_FILE_PATH));
        Map<String, Object> general = (Map<String, Object>) configFile.get("general");
        Map<String, Object> gridGainConfig = (Map<String, Object>) general.get("gridgain");


        Grid grid = null;
        String cache = null;
        List<String> _gridGainServers = null;
        Map<String, Object> _s3Config = null;

        try {

            OptionParser parser = new OptionParser("c::k::");

            OptionSet options = parser.parse(args);


            if (options.hasArgument("c")) {
                cache = (String) options.valueOf("c");

                GridCacheConfiguration cacheConf = new GridCacheConfiguration();
                cacheConf.setDistributionMode(GridCacheDistributionMode.CLIENT_ONLY);

                GridConfiguration conf = new GridConfiguration();



                if(!gridGainConfig.containsKey("s3")) {
                    _gridGainServers = (List<String>) gridGainConfig.get("servers");
                }
                else{
                    _s3Config = (Map<String, Object>) gridGainConfig.get("s3");
                }

                GridTcpDiscoverySpi gridTcp = new GridTcpDiscoverySpi();

                if(_s3Config==null) {
                    GridTcpDiscoveryVmIpFinder gridIpFinder = new GridTcpDiscoveryVmIpFinder();

                    Collection<InetSocketAddress> ips = new ArrayList<InetSocketAddress>();

                    try {
                        conf.setLocalHost(InetAddress.getLocalHost().getHostName());
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    }

                    if (_gridGainServers != null) {
                        for (String server : _gridGainServers) {
                            String[] serverPort = server.split(":");
                            ips.add(new InetSocketAddress(serverPort[0], Integer.valueOf(serverPort[1])));
                        }

                        gridIpFinder.registerAddresses(ips);
                    }

                    gridTcp.setIpFinder(gridIpFinder);

                } else {
                    GridTcpDiscoveryS3IpFinder s3IpFinder = new GridTcpDiscoveryS3IpFinder();
                    s3IpFinder.setBucketName(_s3Config.get("bucket").toString());
                    s3IpFinder.setAwsCredentials(new BasicAWSCredentials(_s3Config.get("access_key").toString(), _s3Config.get("secret_key").toString()));
                    gridTcp.setIpFinder(s3IpFinder);
                }

                conf.setDiscoverySpi(gridTcp);


                cacheConf.setCacheMode(GridCacheMode.PARTITIONED);
                cacheConf.setName(cache);
                if (cache.equals("darklist"))
                    cacheConf.setBackups(1);


                conf.setCacheConfiguration(cacheConf);


                grid = GridGain.start(conf);
                GridCache<String, Map<String, Object>> map = null;

                map = grid.cache(cache);


                if (options.hasArgument("k")) {
                    String value = " " + map.get((String) options.valueOf("k")).toString() + " ";

                    String head = " Cache Key: " + cache + ":" + options.valueOf("k") + " ";
                    printLine(value.length() + 2);

                    colorize(Colorize.ANSI_BLUE);
                    System.out.println(head);
                    colorize(Colorize.ANSI_RESET);

                    printLine(value.length() + 2);

                    colorize(Colorize.ANSI_BLUE);
                    System.out.println(value);
                    colorize(Colorize.ANSI_RESET);

                    printLine(value.length() + 2);


                }
                grid.close();
            }

        } catch (Exception ex) {
            colorize(Colorize.ANSI_RESET);
            try {
                grid.close();
                System.out.println("Key not found on cache: " + cache);


            } catch (Exception eGrid) {
                System.out.println("Key not found on cache: " + cache);
            }
        }

    }

    public static void printLine(Integer num) {
        colorize(Colorize.ANSI_CYAN);
        for (int i = 0; i < num; i++) {
            System.out.print("-");
        }
        colorize(Colorize.ANSI_RESET);
        System.out.println();
    }

    public static void colorize(String color) {
        System.out.print(color);
    }

}
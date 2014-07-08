package net.redborder.utils.gridgain;

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

import java.util.Map;

/**
 * Created by andresgomez on 07/07/14.
 */
public class Key {

    public static void main(String[] args) throws GridException {

        try {

            OptionParser parser = new OptionParser("c::k::");

            OptionSet options = parser.parse(args);

            String cache = null;

            if (options.hasArgument("c")) {
                cache = (String) options.valueOf("c");

                GridCacheConfiguration cacheConf = new GridCacheConfiguration();
                cacheConf.setDistributionMode(GridCacheDistributionMode.CLIENT_ONLY);


                GridConfiguration conf = new GridConfiguration();
                cacheConf.setCacheMode(GridCacheMode.PARTITIONED);
                cacheConf.setName(cache);
                cacheConf.setBackups(1);


                conf.setCacheConfiguration(cacheConf);


                Grid grid = GridGain.start(conf);
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
            System.out.println(ex);
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
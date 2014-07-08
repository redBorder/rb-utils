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
        Grid grid = null;

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


                grid = GridGain.start(conf);
                GridCache<String, Map<String, Object>> map = null;

                try {
                    map = grid.cache(cache);
                } catch (Exception ex) {
                    colorize(Colorize.ANSI_RED);
                    System.out.println("Dont found cache: " + cache + "\n");
                    colorize(Colorize.ANSI_RESET);
                    System.exit(1);
                }

                System.out.println();

                if (options.hasArgument("k")) {
                    try {
                        String value = " " + map.get((String) options.valueOf("k")).toString() + " ";

                        String head = " Cache: " + cache + "  " + " Key: " + options.valueOf("k") + " ";
                        printLine(head.length() + 2);
                        colorize(Colorize.ANSI_CYAN);
                        System.out.print("|");
                        colorize(Colorize.ANSI_BLUE);
                        System.out.print(head);
                        colorize(Colorize.ANSI_CYAN);
                        System.out.println("|");
                        printLine(value.length() + 2);
                        colorize(Colorize.ANSI_CYAN);

                        System.out.print("|");
                        colorize(Colorize.ANSI_BLUE);

                        System.out.print(value);
                        colorize(Colorize.ANSI_CYAN);

                        System.out.println("|");

                        printLine(value.length() + 2);

                    } catch (Exception ex) {
                        colorize(Colorize.ANSI_RED);
                        System.out.print("Dont found key: " + options.valueOf("k").toString());
                        colorize(Colorize.ANSI_RESET);
                    }


                } else {
                    colorize(Colorize.ANSI_RED);
                    System.out.print("You must use -c={cache} -k={value}");
                    colorize(Colorize.ANSI_RESET);
                }

                colorize(Colorize.ANSI_RESET);
                System.out.println();
                grid.close();
            }
            else{
                colorize(Colorize.ANSI_RED);
                System.out.println("You must use -c={cache} -k={value}");
                colorize(Colorize.ANSI_RESET);
            }
        } catch (Exception ex) {
            colorize(Colorize.ANSI_RESET);
            System.out.println(ex);

            grid.close();
        }

    }

    public static void printLine(Integer num) {
        colorize(Colorize.ANSI_CYAN);
        for (int i = 0; i < num; i++) {
            System.out.print("-");
        }
        System.out.println();
        colorize(Colorize.ANSI_RESET);
    }

    public static void colorize(String color) {
        System.out.print(color);
    }

}
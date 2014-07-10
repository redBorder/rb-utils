package net.redborder.utils.darklist;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.RetryOneTime;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.zookeeper.CreateMode;
import org.gridgain.grid.Grid;
import org.gridgain.grid.GridConfiguration;
import org.gridgain.grid.GridGain;
import org.gridgain.grid.cache.GridCache;
import org.gridgain.grid.cache.GridCacheConfiguration;
import org.gridgain.grid.cache.GridCacheDistributionMode;
import org.gridgain.grid.cache.GridCacheMode;
import org.ho.yaml.Yaml;


import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

/**
 * Created by andresgomez on 29/06/14.
 */
public class DarkListUpdate {

    private final static String CONFIG_FILE_PATH = "/opt/rb/etc/darklist_config.yml";


    static Logger log = Logger.getLogger("DarkListUpdate");
    static CuratorFramework client = null;
    static Integer timeout=10000;

    public static void main(String[] args) {


        try {

            Map<String, Object> configMap = (Map<String, Object>) Yaml.load(new File(CONFIG_FILE_PATH));

            Map<String, Object> general = (Map<String, Object>) configMap.get("general");

            boolean allList = true;


            client = CuratorFrameworkFactory.newClient(general.get("zookeeper").toString(), new RetryOneTime(1000));

            client.start();

            if (client.checkExists().forPath("/darklist") == null) {
                client.create().forPath("/darklist");
            }

            Random rand = new Random();
            Thread.sleep(rand.nextInt(1000) + 500);

            if (client.checkExists().forPath("/darklist/barrier") != null) {
                log.log(Level.INFO, "The darkListUpdate daemon is running in other node (locked)");
                System.exit(0);
            } else {
                client.create().withMode(CreateMode.EPHEMERAL).forPath("/darklist/barrier");
                log.log(Level.INFO, "Start darkListUpdate daemon ...");
            }

            if (client.checkExists().forPath("/darklist/lastUpdate") == null) {
                client.create().creatingParentsIfNeeded().forPath("/darklist/lastUpdate");
                String bytes = "" + System.currentTimeMillis() / 1000;
                client.setData().forPath("/darklist/lastUpdate", bytes.getBytes());
                allList = true;
                log.log(Level.INFO, "Start darkListUpdate daemon ...");
                log.log(Level.INFO, "Saved timestamp - Will download full darkList.");
            } else {

                byte[] bytes = client.getData().forPath("/darklist/lastUpdate");
                long diff = System.currentTimeMillis() / 1000 - Long.parseLong(String.valueOf(new String(bytes, "UTF-8")));

                timeout = (Integer) general.get("timeout");
                if (diff > timeout) {
                    allList = true;
                    log.log(Level.INFO, "Incremental time: " + diff + " (limit: " +(Integer) general.get("timeout") + ")");
                    log.log(Level.INFO,"Will download full darkList");
                } else {
                    log.log(Level.INFO, "Incremental time: " + diff + " (limit: " +(Integer) general.get("timeout") + ")");
                    log.log(Level.INFO, "Will download incremental darkList");
                    allList = false;
                }

                String bytesToUpdate = "" + System.currentTimeMillis() / 1000;
                log.log(Level.INFO, "Updating timestamp.");
                client.setData().forPath("/darklist/lastUpdate", bytesToUpdate.getBytes());
            }


            log.log(Level.INFO, "Set barrier: on");

            log.log(Level.INFO, "Downloading darklist ...");

            HttpClient httpclient = HttpClients.createDefault();
            HttpPost httppost = new HttpPost("http://darklist.ipviking.net/slice/");

            // Request parameters and other properties.
            List<NameValuePair> params = new ArrayList<NameValuePair>(2);
            params.add(new BasicNameValuePair("apikey", general.get("api_key").toString()));

            if (allList)
                params.add(new BasicNameValuePair("method", "full"));


            httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));

            //Execute and get the response.
            HttpResponse response = httpclient.execute(httppost);
            HttpEntity entity = response.getEntity();

            InputStream instream = entity.getContent();


            GZIPInputStream in = new GZIPInputStream(instream);


            ByteArrayOutputStream output = new ByteArrayOutputStream();
            // Transfer bytes from the compressed file to the output file
            byte[] buffer = new byte[1024];
            int len;
            while ((len = in.read(buffer)) > 0) {
                output.write(buffer, 0, len);
            }
            in.close();
            output.close();


            log.log(Level.INFO, "Done!");


            Map<Integer, String> category = new HashMap<Integer, String>();
            category.put(0, "<none>");
            category.put(1, "explicit Content");
            category.put(2, "bogon Unadv");
            category.put(3, "bogon Unass");
            category.put(4, "proxy");
            category.put(5, "botnet");
            category.put(6, "financial");
            category.put(7, "cyberTerrorism");
            category.put(8, "identity");
            category.put(9, "bruteForce");
            category.put(10, "cyber Stalking");
            category.put(11, "arms");
            category.put(12, "drugs");
            category.put(13, "espionage");
            category.put(14, "music piracy");
            category.put(15, "games piracy");
            category.put(16, "movie piracy");
            category.put(17, "publishing piracy");
            category.put(18, "stockMarket");
            category.put(19, "hacked");
            category.put(20, "information piracy");
            category.put(21, "high risk");
            category.put(22, "HTTP");
            category.put(31, "malicious site");
            category.put(41, "friendly scanning");
            category.put(51, "web attacks");
            category.put(61, "data harvesting");
            category.put(71, "global whitelist");
            category.put(81, "malware");
            category.put(82, "passive DNS");


            Map<Integer, String> protocol = new HashMap<Integer, String>();
            protocol.put(0, "<none>");
            protocol.put(7, "eDonkey user");
            protocol.put(17, "ares user");
            protocol.put(19, "gnutella user");
            protocol.put(30, "IP unassigned");
            protocol.put(31, "IP unadvertised");
            protocol.put(32, "tor exit");
            protocol.put(33, "IP based proxy");
            protocol.put(34, "web proxy");
            protocol.put(40, "botnet CC");
            protocol.put(41, "bot");
            protocol.put(50, "carding user");
            protocol.put(51, "carding generator");
            protocol.put(52, "financial fraudster ");
            protocol.put(54, "insider information leak");
            protocol.put(55, "merchant submitted fraud");
            protocol.put(60, "intelligence collector");
            protocol.put(70, "identity phishing");
            protocol.put(80, "brute force attacker");
            protocol.put(90, "cyber Stalker");
            protocol.put(100, "arms dealer site");
            protocol.put(110, "mule recruitment");
            protocol.put(111, "narcotics Soliciting");
            protocol.put(112, "pharmaceutical soliciting");
            protocol.put(120, "industrial Espionage Server");
            protocol.put(130, "hacked Computer");
            protocol.put(140, "pump and dump user");
            protocol.put(141, "short scalping computer");
            protocol.put(150, "high risk user TMI");
            protocol.put(151, "high risk network");
            protocol.put(152, "high risk site");
            protocol.put(153, "porn content");
            protocol.put(156, "gambling");
            protocol.put(157, "chat");
            protocol.put(158, "web radio");
            protocol.put(159, "webmail");
            protocol.put(160, "warez");
            protocol.put(161, "shopping");
            protocol.put(162, "advertisement");
            protocol.put(163, "movies");
            protocol.put(164, "violence");
            protocol.put(165, "music");
            protocol.put(166, "hacking");
            protocol.put(167, "ISP");
            protocol.put(168, "drugs");
            protocol.put(169, "agressive");
            protocol.put(170, "news");
            protocol.put(171, "redirector");
            protocol.put(172, "spyware");
            protocol.put(173, "dating");
            protocol.put(174, "dynamic");
            protocol.put(175, "job search");
            protocol.put(176, "tracker");
            protocol.put(177, "models");
            protocol.put(178, "forum");
            protocol.put(179, "web TV");
            protocol.put(180, "downloads");
            protocol.put(181, "ring tones");
            protocol.put(182, "search engine");
            protocol.put(183, "social net");
            protocol.put(184, "update sites");
            protocol.put(185, "weapons");
            protocol.put(186, "web phone");
            protocol.put(187, "religion ");
            protocol.put(188, "image hosting");
            protocol.put(189, "podcast");
            protocol.put(190, "hospitals");
            protocol.put(191, "military");
            protocol.put(192, "politics");
            protocol.put(193, "remote control");
            protocol.put(194, "fortune telling");
            protocol.put(195, "library");
            protocol.put(196, "cost traps");
            protocol.put(197, "homestyle");
            protocol.put(198, "government");
            protocol.put(199, "alcohol");
            protocol.put(200, "radio TV");
            protocol.put(201, "zeus bot");
            protocol.put(211, "butterflies bot");
            protocol.put(221, "keylogger bot");
            protocol.put(231, "zeroaccess bot");
            protocol.put(241, "palevo bot");
            protocol.put(251, "ICE XX bot");
            protocol.put(261, "SpyEye bot");
            protocol.put(271, "drive by malware");
            protocol.put(281, "binary site");
            protocol.put(291, "DDOS scatter");
            protocol.put(301, "port scan scatter");
            protocol.put(311, "SPAM scatter");
            protocol.put(321, "iframe hidden");
            protocol.put(331, "iframe injected");
            protocol.put(341, ".htaccess redirect");
            protocol.put(351, "bad javascript");
            protocol.put(361, "phising site");
            protocol.put(371, "friendly port scan");
            protocol.put(381, "manual WL entry");
            protocol.put(391, "VPN anchorfree.com");
            protocol.put(392, "malware URL");
            protocol.put(393, "malware domain");

            log.log(Level.INFO, "Parsers CSV data ...");



            Reader readerCsv = new StringReader(output.toString());

            CSVReader reader = new CSVReader(readerCsv);
            List<String> keysToDelete = new LinkedList<String>();
            List<String[]> csv = reader.readAll();

            List<Map> dataToSave = new ArrayList<Map>();
            List<String> keysToSave = new ArrayList<String>();


            for (int i = 1; i < csv.size(); i++) {

                String[] nextLine = csv.get(i);

                if (nextLine[0].equals("-")) {
                    keysToDelete.add(nextLine[1]);
                } else {
                    Map<String, String> map = new HashMap<String, String>();
                    map.put("darklist_score", nextLine[2].toString());

                    Double scorePercent = Double.parseDouble(nextLine[2].toString());

                    if (100 >= scorePercent && scorePercent > 95) {
                        map.put("darklist_score_name", "very high");
                    } else if (95 >= scorePercent && scorePercent > 85) {
                        map.put("darklist_score_name", "high");
                    } else if (85 >= scorePercent && scorePercent > 70) {
                        map.put("darklist_score_name", "medium");
                    } else if (70 >= scorePercent && scorePercent > 50) {
                        map.put("darklist_score_name", "low");
                    } else if (50 >= scorePercent && scorePercent >= 20) {
                        map.put("darklist_score_name", "very low");
                    }

                    if(category.get(Integer.parseInt(nextLine[6]))!=null) {
                        map.put("darklist_category", category.get(Integer.parseInt(nextLine[6])));
                    }else{
                        map.put("darklist_category", "Unknown: " + nextLine[6]);
                    }

                    if(protocol.get(Integer.parseInt(nextLine[7]))!=null) {
                        map.put("darklist_protocol", protocol.get(Integer.parseInt(nextLine[7])));
                    }else{
                        map.put("darklist_protocol", "Unknown: " + nextLine[7]);
                    }

                    //map.put("darklist_lat", nextLine[3].toString());
                    //map.put("darklist_long", nextLine[4].toString());
                    //map.put("darklist_country", nextLine[5].toString());
                    //map.put("darklist_last_seen", nextLine[8].toString());

                    keysToSave.add(nextLine[1].toString());

                    dataToSave.add(map);

                }
            }

            log.log(Level.INFO, "Done!");


            Integer partitionsSave = keysToSave.size() / 4;
            Integer partitionDelete = keysToDelete.size() / 4;


            log.log(Level.INFO,"Keys by to save: " + keysToSave.size());
            log.log(Level.INFO,"Keys by to delete: " + keysToDelete.size());

            log.log(Level.INFO,"Keys by thread to save: " + partitionsSave);
            log.log(Level.INFO,"Keys by thread to delete: " + partitionDelete);

            GridCacheConfiguration cacheConf = new GridCacheConfiguration();
            cacheConf.setDistributionMode(GridCacheDistributionMode.CLIENT_ONLY);


            GridConfiguration conf = new GridConfiguration();
            cacheConf.setCacheMode(GridCacheMode.PARTITIONED);
            cacheConf.setName("darklist");
            cacheConf.setBackups(1);


            conf.setCacheConfiguration(cacheConf);


            Grid grid = GridGain.start(conf);



            if (allList) {
                GridCache<String, Map<String, Object>> map = grid.cache("darklist");
                map.globalClearAll();
                log.log(Level.INFO,"Cleaning old darkList ...");
                Thread.sleep(2000);
            }


            List<GgClientThread> threads = new ArrayList<GgClientThread>();

            for (int j = 0; j < 4; j++) {

                GgClientThread ggThread = new GgClientThread(j, partitionsSave,
                        keysToSave, dataToSave, partitionDelete, keysToDelete, grid);
                ggThread.start();
                threads.add(ggThread);
            }

            for (int i = 0; i < threads.size(); i++) {
                log.log(Level.INFO,"Waitting thread [" + i + "] ...");
                threads.get(i).join();
            }

            grid.close();

            client.close();
            log.log(Level.INFO,"Set barrier: off");
            log.log(Level.INFO,"\nDarklist updated!");



        } catch (Exception ex) {
            log.log(Level.SEVERE,"EXCEPTION! ", ex.toString());
            String bytesToUpdate = "" + (timeout+timeout);
            try {
                client.setData().forPath("/darklist/lastUpdate", bytesToUpdate.getBytes());
            } catch (Exception e) {
                e.printStackTrace();
            }
            client.close();
            System.exit(1);
        }

    }
}

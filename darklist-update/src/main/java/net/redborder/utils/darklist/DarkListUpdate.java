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
            category.put(1, "Explicit Content");
            category.put(2, "Bogon Unadv");
            category.put(3, "Bogon Unass");
            category.put(4, "Proxy");
            category.put(5, "Botnet");
            category.put(6, "Financial");
            category.put(7, "CyberTerrorism");
            category.put(8, "Identity");
            category.put(9, "BruteForce");
            category.put(10, "Cyber Stalking");
            category.put(11, "Arms");
            category.put(12, "Drugs");
            category.put(13, "Espionage");
            category.put(14, "Music Piracy");
            category.put(15, "Games piracy");
            category.put(16, "Movie piracy");
            category.put(17, "Publishing piracy");
            category.put(18, "StockMarket");
            category.put(19, "Hacked");
            category.put(20, "Information piracy");
            category.put(21, "High risk");
            category.put(22, "HTTP");
            category.put(31, "Malicious Site");
            category.put(41, "Friendly Scanning");
            category.put(51, "Web Attacks");
            category.put(61, "DATA Harvesting");
            category.put(71, "Global Whitelist");
            category.put(81, "Malware");
            category.put(82, "Passive DNS");


            Map<Integer, String> protocol = new HashMap<Integer, String>();
            protocol.put(0, "<none>");
            protocol.put(7, "eDonkey User");
            protocol.put(17, "Ares User");
            protocol.put(19, "Gnutella User");
            protocol.put(30, "IP unassigned");
            protocol.put(31, "IP unadvertised");
            protocol.put(32, "Tor Exit");
            protocol.put(33, "IP based proxy");
            protocol.put(34, "Web Proxy");
            protocol.put(40, "Botnet CC");
            protocol.put(41, "Bot");
            protocol.put(50, "Carding user");
            protocol.put(51, "Carding Generator");
            protocol.put(52, "Financial Fraudster ");
            protocol.put(54, "Insider Information Leak");
            protocol.put(55, "Merchant Submitted fraud");
            protocol.put(60, "Intelligence collector");
            protocol.put(70, "Identity Phishing");
            protocol.put(80, "Brute Force attacker");
            protocol.put(90, "Cyber Stalker");
            protocol.put(100, "Arms dealer site");
            protocol.put(110, "Mule recruitment");
            protocol.put(111, "Narcotics Soliciting");
            protocol.put(112, "Pharmaceutical soliciting");
            protocol.put(120, "Industrial Espionage Server");
            protocol.put(130, "Hacked Computer");
            protocol.put(140, "Pump and dump user");
            protocol.put(141, "Short Scalping Computer");
            protocol.put(150, "High risk user TMI");
            protocol.put(151, "High risk Network");
            protocol.put(152, "High risk site");
            protocol.put(153, "Porn Content");
            protocol.put(156, "Gambling");
            protocol.put(157, "Chat");
            protocol.put(158, "Web Radio");
            protocol.put(159, "Webmail");
            protocol.put(160, "Warez");
            protocol.put(161, "Shopping");
            protocol.put(162, "Advertisement");
            protocol.put(163, "Movies");
            protocol.put(164, "Violence");
            protocol.put(165, "Music");
            protocol.put(166, "Hacking");
            protocol.put(167, "ISP");
            protocol.put(168, "Drugs");
            protocol.put(169, "Agressive");
            protocol.put(170, "News");
            protocol.put(171, "Redirector");
            protocol.put(172, "Spyware");
            protocol.put(173, "Dating");
            protocol.put(174, "Dynamic");
            protocol.put(175, "Job Search");
            protocol.put(176, "Tracker");
            protocol.put(177, "Models");
            protocol.put(178, "Forum");
            protocol.put(179, "Web TV");
            protocol.put(180, "Downloads");
            protocol.put(181, "Ring Tones");
            protocol.put(182, "Search Engine");
            protocol.put(183, "Social Net");
            protocol.put(184, "Update Sites");
            protocol.put(185, "Weapons");
            protocol.put(186, "Web Phone");
            protocol.put(187, "Religion ");
            protocol.put(188, "Image Hosting");
            protocol.put(189, "Podcast");
            protocol.put(190, "Hospitals");
            protocol.put(191, "Military");
            protocol.put(192, "Politics");
            protocol.put(193, "Remote control");
            protocol.put(194, "Fortune Telling");
            protocol.put(195, "Library");
            protocol.put(196, "Cost Traps");
            protocol.put(197, "Homestyle");
            protocol.put(198, "Government");
            protocol.put(199, "Alcohol");
            protocol.put(200, "Radio TV");
            protocol.put(201, "Zeus Bot");
            protocol.put(211, "Butterflies Bot");
            protocol.put(221, "Keylogger Bot");
            protocol.put(231, "Zeroaccess Bot");
            protocol.put(241, "Palevo Bot");
            protocol.put(251, "ICE XX Bot");
            protocol.put(261, "SpyEye Bot");
            protocol.put(271, "DriveByMalware");
            protocol.put(281, "Binary Site");
            protocol.put(291, "DDOS scatter");
            protocol.put(301, "Port Scan Scatter");
            protocol.put(311, "SPAM Scatter");
            protocol.put(321, "Iframe Hidden");
            protocol.put(331, "Iframe Injected");
            protocol.put(341, ".htaccess redirect");
            protocol.put(351, "Bad Javascript");
            protocol.put(361, "Phising Site");
            protocol.put(371, "Friendly Port scan");
            protocol.put(381, "Manual WL entry");
            protocol.put(391, "VPN AnchorFree.com");
            protocol.put(392, "Malware URL");
            protocol.put(393, "Malware domain");

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

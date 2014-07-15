package net.redborder.utils.darklist;

import org.gridgain.grid.Grid;
import org.gridgain.grid.GridException;
import org.gridgain.grid.cache.GridCache;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by andresgomez on 29/06/14.
 */
public class GgClientThread extends Thread {


    Integer _toSave;
    List<String> _keyToSave;
    List<Map> _dataToSave;
    List<String> _keyToDelete;
    Integer _toDelete;
    Grid _client;
    Integer _index;
    GridCache<String, Map<String, Object>> map;


    public GgClientThread(Integer index, Integer toSave, List<String> keyToSave, List<Map> dataToSave, Integer toDelete, List<String> keyToDelete, Grid client) {
        _toSave = toSave;
        _keyToSave = keyToSave;
        _dataToSave = dataToSave;
        _toDelete = toDelete;
        _keyToDelete = keyToDelete;
        _index = index;

        _client = client;

        map = client.cache("darklist");


    }


    public void run() {

        int porcent = 0;

        if (_toDelete != 0) {
            System.out.println("Deleting : [ " + (_index * _toDelete) + " - " + (_toDelete * (_index + 1) - 1) + " ] -> " + new Date().toString());

            try {
                map.removeAll(_keyToDelete);
            } catch (GridException e) {
                e.printStackTrace();
            }

        } else
            System.out.println("Nothing to delete!");


        Map<String, Map<String, Object>> mapToSave = new HashMap<String, Map<String, Object>>();


        System.out.println("Saving : [ " + (_index * _toSave) + " - " + (_toSave * (_index + 1) - 1) + " ] -> " + new Date().toString());
        porcent = 0;

        for (int i = (_index * _toSave); i < _toSave * (_index + 1); i++) {
        /*
            if (i == (_toSave * (_index + 1) * porcent / 100) + (_index * _toSave)) {
                System.out.println(_keyToSave.get(i) + " -- " + new Date().toString() + " -- [ " + _index + " ] -- Intervalo:" + " [ " + i + " - " + (_toSave * (_index + 1) - 1) + " ] " + "-> " + porcent + " %");
                porcent++;
            }
            */

            mapToSave.put(_keyToSave.get(i), _dataToSave.get(i));
        }


        try {
            map.putAll(mapToSave);
            System.out.println("Compacting data on [" +_index + "] ...");
            map.compactAll();
        } catch (GridException e) {
            e.printStackTrace();
        }



        System.out.println("[ " + _index + " ] Done!: [ " + (_index * _toSave) + " - " + (_toSave * (_index + 1) - 1) + " ]");


    }
}


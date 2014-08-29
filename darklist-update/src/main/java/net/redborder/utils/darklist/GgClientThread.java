package net.redborder.utils.darklist;

import org.gridgain.grid.Grid;
import org.gridgain.grid.GridException;
import org.gridgain.grid.cache.GridCache;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The threads that update the data in the darklist GridGain cache.
 * @author Andres Gomez
 */
public class GgClientThread extends Thread {

    /**
     * Number of keys to save.
     */
    Integer _toSave;
    /**
     * A list with the keys to save.
     */
    List<String> _keyToSave;
    /**
     * A list with the data to save.
     */
    List<Map> _dataToSave;
    /**
     * A list with the keys to delete.
     */
    List<String> _keyToDelete;
    /**
     * Numer of keys to delete.
     */
    Integer _toDelete;
    /**
     * Client to GridGain cluster.
     */
    Grid _client;
    /**
     * The start of the list.
     */
    Integer _index;

    /**
     * The darklist cache.
     */
    GridCache<String, Map<String, Object>> map;


    /**
     * Constructor
     * @param index The start index of the list.
     * @param toSave The number of elements to save.
     * @param keyToSave The list with the keys to save.
     * @param dataToSave The list with the data to save.
     * @param toDelete The number of elements to delete.
     * @param keyToDelete The list with the elements to delete.
     * @param client The GridGain client.
     */
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


    /**
     * The execution of the Thread.
     */
    public void run() {

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

        for (int i = (_index * _toSave); i < _toSave * (_index + 1); i++) {
            mapToSave.put(_keyToSave.get(i), _dataToSave.get(i));
        }

        try {
            map.putAll(mapToSave);
            System.out.println("Saved: " + mapToSave.size());
            System.out.println("Compacting data on [" +_index + "] ...");
            map.compactAll();
        } catch (GridException e) {
            e.printStackTrace();
        }



        System.out.println("[ " + _index + " ] Done!: [ " + (_index * _toSave) + " - " + (_toSave * (_index + 1) - 1) + " ]");


    }
}


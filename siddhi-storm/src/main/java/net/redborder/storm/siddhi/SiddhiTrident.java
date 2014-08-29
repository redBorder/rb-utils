package net.redborder.storm.siddhi;

import backtype.storm.tuple.Values;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.query.Query;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by andresgomez on 19/06/14.
 */
public class SiddhiTrident extends BaseFunction {

    transient SiddhiManager _siddhiManager;
    SiddhiExecutionPlan _executionPlan;
    List<InputHandler> _inputHandler;
    List<StreamDefinition> _inputStreams;
    String _queryReference;

    private TridentCollector _collector;


    public SiddhiTrident(SiddhiExecutionPlan executionPlan) {
        _executionPlan = executionPlan;
        _inputStreams = new ArrayList<StreamDefinition>();
        _inputHandler = new ArrayList<InputHandler>();
    }

    @Override
    public void prepare(java.util.Map conf, storm.trident.operation.TridentOperationContext context) {

        boolean existInputStream = false;
        boolean existOutputStream = false;

        SiddhiConfiguration configuration = new SiddhiConfiguration();
        configuration.setDistributedProcessing(true);
        configuration.setQueryPlanIdentifier(_executionPlan._hazelCastInstance);

         _siddhiManager = new SiddhiManager(configuration);


        if (_executionPlan.inputStreamName == null) {
            System.out.println("You must use inputStreamName on config");
            System.exit(1);
        }

        for (SiddhiStream stream : _executionPlan.streams.values()) {

            if (stream._isInputStream) {
                existInputStream = true;
                _inputStreams.add(stream.streamDefinition);
            }
            _siddhiManager.defineStream(stream.streamDefinition);
        }

        if (!existInputStream) {
            System.out.println("The input stream: " + _executionPlan.inputStreamName + " not exist on the streams!");
            System.exit(1);
        }

        for (String query : _executionPlan.querys) {
            try {
                if (query.contains(_executionPlan.outputStreamName)) {
                    _queryReference = _siddhiManager.addQuery(query);
                    existOutputStream = true;
                } else {
                    _siddhiManager.addQuery(query);
                }
            } catch (SiddhiParserException ex) {
                System.out.println("Invalid query expresion: \n [ " + query + " ]\n this query not added!");
            }
        }

        if (!existOutputStream) {
            System.out.println("The output stream: " + _executionPlan.outputStreamName + " not exist on the querys!");
        }

        for(String inputStreamName : _executionPlan.inputStreamName)
        _inputHandler.add(_siddhiManager.getInputHandler(inputStreamName));

        _siddhiManager.addCallback(_queryReference, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    Map<String, Object> result = new HashMap<String, Object>();
                    Object[] data = event.getData();
                    for (int i = 0; i < _executionPlan.outPutEventNames.size(); i++) {
                        result.put(_executionPlan.outPutEventNames.get(i), data[i]);
                    }
                    _collector.emit(new Values(result));
                }

            }
        });    }

    @Override
    public void execute(TridentTuple tuple, final TridentCollector collector) {

        _collector=collector;

        if (tuple.size() == _inputStreams.size()) {
            for (int i = 0; i < tuple.size(); i++) {
                Map<String, Object> map = (Map<String, Object>) tuple.get(i);

                try {
                    _inputHandler.get(i).send(mapToArray(map,i).toArray());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } else {
            System.out.println(" Different size tuple[" + tuple.size() +"] and inputStream [" + _inputStreams.size() +"]");
        }

    }

    private List<Object> mapToArray(Map<String, Object> map, int index){
        List<Object> event = new ArrayList<Object>();

        for (Attribute field : _inputStreams.get(index).getAttributeList()) {
            event.add(map.get(field.getName()));
        }

        return event;
    }
}

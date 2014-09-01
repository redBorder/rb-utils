package net.redborder.storm.siddhi;

import backtype.storm.tuple.Values;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
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
    List<InputHandler> _inputHandler;
    List<StreamDefinition> _inputStreams;
    String _queryReference;

    private TridentCollector _collector;



    @Override
    public void prepare(java.util.Map conf, storm.trident.operation.TridentOperationContext context) {

        final SiddhiExecutionPlan  executionPlan = new SiddhiExecutionPlan("stormTest");

        executionPlan
                .newStream("streamA", true)
                    .addParameter("src", "string")
                    .addParameter("bytes", "int")
                .buildStream()
                .addQuery("from streamA[src == '192.168.1.100' and bytes > 150] select src, bytes insert into windowStream")
                .addQuery("from windowStream[bytes > 250] insert into outStream2")
                .addQuery("from windowStream#window.time(1 min) select avg(bytes) as avgBytes, max(bytes) as maxBytes, min(bytes) as minBytes, src, bytes insert into outStream for current-events")
                .addOutputStreamName("outStream")
                    .addOutPutEventName("avgBytes")
                    .addOutPutEventName("maxBytes")
                    .addOutPutEventName("minBytes")
                    .addOutPutEventName("src")
                    .addOutPutEventName("bytes")
                .buildOutPutStream()
                .addOutputStreamName("outStream2")
                    .addOutPutEventName("src")
                    .addOutPutEventName("bytes")
                .buildOutPutStream();


        _inputStreams = new ArrayList<StreamDefinition>();
        _inputHandler = new ArrayList<InputHandler>();

        SiddhiConfiguration configuration = new SiddhiConfiguration();
        configuration.setDistributedProcessing(true);
        configuration.setQueryPlanIdentifier(executionPlan._hazelCastInstance);

         _siddhiManager = new SiddhiManager(configuration);


        if (executionPlan.inputStreamName == null) {
            System.out.println("You must use inputStreamName on config");
            System.exit(1);
        }

        for (SiddhiStream stream : executionPlan.streams.values()) {

            if (stream._isInputStream) {
                _inputStreams.add(stream.streamDefinition);
            }
            _siddhiManager.defineStream(stream.streamDefinition);
        }

        if (executionPlan.inputStreamName.isEmpty()) {
            System.out.println("The input stream: " + executionPlan.inputStreamName + " not exist on the streams!");
            System.exit(1);
        }

        for (String query : executionPlan.querys) {
            try {
                    _siddhiManager.addQuery(query);
            } catch (SiddhiParserException ex) {
                System.out.println("Invalid query expresion: \n [ " + query + " ]\n this query not added!");
            }
        }

        if (executionPlan.outputStreamName.isEmpty()) {
            System.out.println("The output stream: " + executionPlan.outputStreamName + " not exist on the querys!");
        }

        for(String inputStreamName : executionPlan.inputStreamName)
        _inputHandler.add(_siddhiManager.getInputHandler(inputStreamName));

        for(final String outputStreamName : executionPlan.outputStreamName) {

            _siddhiManager.addCallback(outputStreamName, new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        Map<String, Object> result = new HashMap<String, Object>();
                        Object[] data = event.getData();
                        for (int i = 0; i < executionPlan.outPutEventNames.size(); i++) {
                            result.put(executionPlan.outPutEventNames.get(i), data[i]);
                        }
                        _collector.emit(new Values(outputStreamName, result));
                    }
                }
            });
        }
    }

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

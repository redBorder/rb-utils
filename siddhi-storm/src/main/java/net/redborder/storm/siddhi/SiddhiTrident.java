package net.redborder.storm.siddhi;

import backtype.storm.tuple.Values;
import com.twitter.concurrent.ThreadPoolScheduler;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.ho.yaml.Yaml;
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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;


/**
 * Created by andresgomez on 19/06/14.
 */
public class SiddhiTrident extends BaseFunction {

    transient SiddhiManager _siddhiManager;
    Map<String, List<InputHandler>> _inputsHandler;
    Map<String, List<StreamDefinition>> _inputStreams;
    String _zookeeper;
    CuratorFramework client = null;
    private final String CONFIG_FILE_PATH = "/opt/rb/etc/rb-bi/queries.yml";
    List<String> _streams;
    Map<String, String> _queries;
    String _inputField;


    private TridentCollector _collector;

    public SiddhiTrident(String zookeeper, String inputField) {
        _zookeeper = zookeeper;
        _inputField = inputField;
    }


    @Override
    public void prepare(java.util.Map conf, storm.trident.operation.TridentOperationContext context) {

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

        client = CuratorFrameworkFactory.newClient(_zookeeper, retryPolicy);

        client.start();

        _streams = new ArrayList<String>();
        _queries = new HashMap<String, String>();

        try {
            if (client.checkExists().forPath("/query-siddhi") == null)
                client.create().forPath("/query-siddhi");
        } catch (Exception e) {
            e.printStackTrace();
        }

        checkSignal();

        parserQuerysFile();

    }

    @Override
    public void execute(TridentTuple tuple, final TridentCollector collector) {

        _collector = collector;

        Map<String, Object> map = (Map<String, Object>) tuple.getValueByField(_inputField);
        for (int i = 0; i < _inputsHandler.get(_inputField).size(); i++) {
            try {
                _inputsHandler.get(_inputField).get(i).send(mapToArray(map, _inputField, i).toArray());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private List<Object> mapToArray(Map<String, Object> map, String inputField, int index) {
        List<Object> event = new ArrayList<Object>();

        for (Attribute field : _inputStreams.get(inputField).get(index).getAttributeList()) {
            event.add(map.get(field.getName()));
        }
        return event;
    }

    public void checkSignal() {

        try {
            client.checkExists().usingWatcher(new CuratorWatcher() {
                @Override
                public void process(WatchedEvent watchedEvent) throws Exception {
                    if (watchedEvent.getType().equals(Watcher.Event.EventType.NodeCreated)) {
                        System.out.println("Updating queries!");
                        _siddhiManager.shutdown();
                        parserQuerysFile();
                        if (client.checkExists().forPath("/query-siddhi/update") != null)
                            client.delete().forPath("/query-siddhi/update");
                    }
                    checkSignal();
                }
            }).forPath("/query-siddhi/update");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void parserQuerysFile() {
        SiddhiExecutionPlan executionPlan = new SiddhiExecutionPlan("storm");

        _inputStreams = new HashMap<String, List<StreamDefinition>>();
        _inputStreams.put("traffics", new ArrayList<StreamDefinition>());
        _inputStreams.put("event", new ArrayList<StreamDefinition>());
        _inputStreams.put("monitor", new ArrayList<StreamDefinition>());

        _inputsHandler = new HashMap<String, List<InputHandler>>();
        _inputsHandler.put("traffics", new ArrayList<InputHandler>());
        _inputsHandler.put("event", new ArrayList<InputHandler>());
        _inputsHandler.put("monitor", new ArrayList<InputHandler>());


        SiddhiConfiguration configuration = new SiddhiConfiguration();
        configuration.setDistributedProcessing(true);
        configuration.setQueryPlanIdentifier(executionPlan._hazelCastInstance);

        _siddhiManager = new SiddhiManager(configuration);

        try {
            Map<String, Object> configExecutionPlan = (Map<String, Object>) Yaml.load(new File(CONFIG_FILE_PATH));
            List<Map<String, Object>> streams = (List<Map<String, Object>>) configExecutionPlan.get("streams");

            for (Map<String, Object> stream : streams) {

                boolean inputStream = false;
                if (stream.containsKey("direction")) {
                    if (stream.get("direction").equals("input"))
                        inputStream = true;
                }

                if (inputStream || !stream.containsKey("direction")) {
                    SiddhiStream streamSiddhi;

                    if (inputStream) {
                        streamSiddhi = executionPlan.newInputStream(stream.get("name").toString(), stream.get("source").toString());
                    } else {
                        streamSiddhi = executionPlan.newStream(stream.get("name").toString());

                    }
                    List<String> parameters = (List<String>) stream.get("parameters");

                    for (String parameter : parameters) {
                        String[] parameterSplit = parameter.split(":");
                        streamSiddhi.addParameter(parameterSplit[0], parameterSplit[1]);
                    }

                    executionPlan = streamSiddhi.buildStream();
                } else {
                    SiddhiOutPutStream outPutSiddhi = executionPlan.addOutputStreamName(stream.get("name").toString());

                    List<String> parameters = (List<String>) stream.get("parameters");

                    for (String parameter : parameters) {
                        outPutSiddhi.addOutPutEventName(parameter);
                    }
                    executionPlan = outPutSiddhi.buildOutPutStream();
                }

                Map<String, String> queries = (Map<String, String>) configExecutionPlan.get("queries");

                List<String> queriesNames = new ArrayList<String>(queries.keySet());
                for (String queryName : queriesNames) {
                    executionPlan.addQuery(queryName, queries.get(queryName));
                }
            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        processExecutionPlan(executionPlan);
    }

    private void processExecutionPlan(final SiddhiExecutionPlan executionPlan) {

        if (executionPlan.inputStreamName.isEmpty()) {
            System.out.println("You must use inputStreamName on config");
            System.exit(1);
        }

        if (executionPlan.outputStreamName.isEmpty()) {
            System.out.println("The output stream: " + executionPlan.outputStreamName + " not exist on the querys!");
            System.exit(1);
        }

        for (SiddhiStream stream : executionPlan.streams.values()) {
            if (stream._isInputStream) {
                _inputStreams.get(stream.source).add(stream.streamDefinition);
            }
            _siddhiManager.defineStream(stream.streamDefinition);
        }

        if (executionPlan.inputStreamName.isEmpty()) {
            System.out.println("The input stream: " + executionPlan.inputStreamName + " not exist on the streams!");
            System.exit(1);
        }

        for (String query : executionPlan.querys.values()) {
            try {
                _siddhiManager.addQuery(query);
            } catch (SiddhiParserException ex) {
                System.out.println("Invalid query expresion: \n [ " + query + " ]\n this query not added!");
            }
        }

        for (String inputStreamName : executionPlan.inputStreamName) {
            String[] inputStream = inputStreamName.split(":");
            _inputsHandler.get(inputStream[1]).add(_siddhiManager.getInputHandler(inputStream[0]));
        }

        for (final String outputStreamName : executionPlan.outputStreamName) {
            _siddhiManager.addCallback(outputStreamName, new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        Map<String, Object> result = new HashMap<String, Object>();
                        Object[] data = event.getData();
                        for (int i = 0; i < executionPlan.outPutEventNames.get(outputStreamName).size(); i++) {
                            result.put(executionPlan.outPutEventNames.get(outputStreamName).get(i), data[i]);
                        }
                        System.out.println("Alert!!! ->  " + result);

                        //_collector.emit(new Values(outputStreamName, result));
                    }
                }
            });
        }
    }
}


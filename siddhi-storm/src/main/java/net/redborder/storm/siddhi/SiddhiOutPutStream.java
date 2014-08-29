package net.redborder.storm.siddhi;

import java.io.Serializable;

/**
 * Created by andresgomez on 22/06/14.
 */
public class SiddhiOutPutStream implements Serializable{
    SiddhiExecutionPlan _executionPlan;

    public SiddhiOutPutStream(String outPutStreamName, SiddhiExecutionPlan executionPlan){
        _executionPlan=executionPlan;
        _executionPlan.outputStreamName=outPutStreamName;
    }

    public SiddhiOutPutStream addOutPutEventName(String eventName){

        if(!_executionPlan.outPutEventNames.contains(eventName)){
            _executionPlan.outPutEventNames.add(eventName);
        }else{
            System.out.println("The event name: "+ eventName + " is already exists!");
        }

        return this;
    }

    public SiddhiExecutionPlan buildOutPutStream(){
        return _executionPlan;
    }
}

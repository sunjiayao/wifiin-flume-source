package com.wifiin.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * @auther sunjiayao on 16/5/14.
 * email: sunjiayaocode@gmail.com
 */
public class WifiinTailFileSource extends AbstractSource implements Configurable, EventDrivenSource {
    private Logger logger = LoggerFactory.getLogger(WifiinTailFileSource.class);
    private WifiinTailFileReader tailFileReader ;
    private volatile boolean runnerFlag = true;
    private int eventSize;
    private SourceCounter sourceCounter;
    public void configure(Context context) {
        eventSize = context.getInteger(WifiinTailFileConfigurationConstants.EVENT_SIZE,WifiinTailFileConfigurationConstants.EVENT_SIZE_DEFAULT);
        try {
            tailFileReader = new WifiinTailFileReader(context);
        } catch (IOException e) {
            logger.error("init file error",e);
            System.exit(0);
        }
        sourceCounter = new SourceCounter(getName());
    }

    @Override
    public synchronized void start() {
        Executors.newSingleThreadExecutor().submit(new TailFileRunnable());
        super.start();
    }

    @Override
    public synchronized void stop() {
        runnerFlag = false;
        super.stop();
    }

    private class TailFileRunnable implements  Runnable{
        public void run() {
            while (runnerFlag){
                logger.debug("tailFile runner");
                try {
                    List<Event> events = null;
                    events = tailFileReader.readEvents(eventSize);
                    if(events!=null && events.isEmpty()){
                        tailFileReader.commit();
                        continue;
                    }

                    logger.debug("read events {}",events.size());
                    sourceCounter.addToEventReceivedCount(events.size());
                    sourceCounter.incrementAppendBatchReceivedCount();
                    getChannelProcessor().processEventBatch(events);
                    tailFileReader.commit();
                    logger.debug("msg commit");
                }catch (Exception e){
                    logger.error("",e);
                }

            }
            if(tailFileReader!=null){
                try {
                    tailFileReader.close();
                } catch (IOException e) {
                    logger.error("close tailFileReader error:",e);
                }
            }
        }
    }
}

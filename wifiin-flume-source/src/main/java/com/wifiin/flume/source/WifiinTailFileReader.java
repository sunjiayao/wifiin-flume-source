package com.wifiin.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.client.avro.ReliableEventReader;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.EventDeserializerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.cs.UTF_32;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static java.lang.String.format;

/**
 * @auther sunjiayao on 16/5/13.
 * email: sunjiayaocode@gmail.com
 */

public class WifiinTailFileReader implements ReliableEventReader {
    private static final Logger logger = LoggerFactory.getLogger(WifiinTailFileReader.class);
    private Charset outputCharset;
    private String fileName;
    private String filePath;
    private int fileLinesSize;
    private Set<FileReadModle> fileReadOffsets = new HashSet<>();
    private BlockingQueue<FileReadModle> logMsgQueue = new LinkedBlockingQueue();
    private ExecutorService fileInputStreamES ;
    private List<Event> events = new ArrayList<>();
    private boolean isCommit = true;
    private String offsetPath;
    private volatile boolean isClose = false;
    private Map<String,WifiinTailFileInputStreamOption> wifiinTailFile = new ConcurrentHashMap<>();
    private WatchService watchService = FileSystems.getDefault().newWatchService();
    private AtomicInteger atomicInteger = new AtomicInteger();
    private int fileSize ;
    public WifiinTailFileReader(Context context) throws IOException {
        configure(context);
        init();
    }
    private void configure(Context context) throws IOException {
        outputCharset=Charset.forName(context.getString(WifiinTailFileConfigurationConstants.FILE_CHARSET,WifiinTailFileConfigurationConstants.FILE_CHARSET_DEFAULT));
        fileName = context.getString(WifiinTailFileConfigurationConstants.FILE_NAME,WifiinTailFileConfigurationConstants.FILE_NAME_DEFAULT);
        filePath = context.getString(WifiinTailFileConfigurationConstants.PATH,WifiinTailFileConfigurationConstants.PATH_DEFAULT);
        offsetPath = context.getString(WifiinTailFileConfigurationConstants.OFFSET_FILE_PATH,WifiinTailFileConfigurationConstants.OFFSET_FILE_PATH_DEFAULT);
        fileLinesSize = context.getInteger(WifiinTailFileConfigurationConstants.FILE_LINES_SIZE,WifiinTailFileConfigurationConstants.FILE_LINES_SIZE_DEFAULT);
        Paths.get(filePath).register(watchService,StandardWatchEventKinds.ENTRY_MODIFY);
    }

    private void init() throws IOException {
        Pattern pattern = Pattern.compile(fileName);
        Set<Path> files = new HashSet<>();
        Files.list(Paths.get(filePath)).forEach(path->{
            if(pattern.matcher(path.getFileName().toString()).find()){
                files.add(path);
                logger.info("find file:{}",path.getFileName().toString());
            }
        });
        fileSize = files.size();
        fileInputStreamES = Executors.newFixedThreadPool(files.size());

        files.forEach(path -> {
            try {
                String offsetName = path.getFileName().toString()+path.getParent().toString().hashCode()+".offset";
                WifiinTailFileInputStreamOption wifiinTailFileInputStreamOption = new WifiinTailFileInputStreamOption(path,Paths.get(this.offsetPath+"/"+offsetName));
                wifiinTailFile.put(path.getFileName().toString(),wifiinTailFileInputStreamOption);
                fileInputStreamES.submit(new FileRead(wifiinTailFileInputStreamOption));
            } catch (IOException e) {
                logger.error("init fileReader error",e);
            }

        });
    }

    public Event readEvent() throws IOException {
        if(!isCommit){
            return events.get(0);
        }
        isCommit = false;
        List<Event> events = readEvents(0);
        return events.isEmpty()? null:events.get(0);
    }

    public List<Event> readEvents(int n) throws IOException {
        logger.debug("begin readEvents.......");

        if(!isCommit){
            return events;
        }
        for(int index = 0;index<n;index++){
            Event event = read();
            if(event==null){
                break;
            }
            events.add(index,event);
        }

        isCommit = false;
        logger.debug("end readEvents.....");
        return events;
    }

    private Event read() throws IOException {
        FileReadModle fileReadModle=null;
        try {
            logger.debug("begin read log..... logMsgQueue size is {} events size is {}",logMsgQueue.size(),events.size());
            while((fileReadModle=logMsgQueue.poll())==null&&events.isEmpty()){
                logger.debug("logMsgQueue is null ,and begin read log");
                boolean hasNext = false;
                synchronized (atomicInteger){
                    for(String key:wifiinTailFile.keySet()){
                        if(wifiinTailFile.get(key).isHasNext()){
                            hasNext = true;
                        }
                        WifiinTailFileInputStreamOption wifiinTailFileInputStreamOption =  wifiinTailFile.get(key);
                        synchronized (wifiinTailFileInputStreamOption){
                            wifiinTailFileInputStreamOption.notify();
                        }
                    }

                    if(hasNext){
                        atomicInteger.wait();
                        continue;
                    }
                }

                synchronized (atomicInteger){
                    WatchKey watchKey = watchService.take();
                    watchKey.pollEvents().forEach(watchEvent -> {
                        if(wifiinTailFile.containsKey(watchEvent.context())){
                            WifiinTailFileInputStreamOption wifiinTailFileInputStreamOption =  wifiinTailFile.get(watchEvent.context());
                            synchronized (wifiinTailFileInputStreamOption){
                                wifiinTailFileInputStreamOption.notify();
                            }
                        }
                    });
                    watchKey.reset();
                    atomicInteger.wait();
                }
            }
        } catch (InterruptedException e) {
            logger.error("log read error",e);
        }

        if(fileReadModle==null){
            return null;
        }

        fileReadOffsets.remove(fileReadModle);
        fileReadOffsets.add(fileReadModle);
        return EventBuilder.withBody(fileReadModle.getMsg(),outputCharset);
    }




    private class FileRead implements Runnable{
        private WifiinTailFileInputStreamOption fileInputStreamOption;
        public FileRead(WifiinTailFileInputStreamOption fileInputStreamOption) throws IOException {
            this.fileInputStreamOption = fileInputStreamOption;
        }
        @Override
        public void run() {
                    try {
                        String msg = null;

                        while(true){
                            while(logMsgQueue.size()>=fileLinesSize||(msg=fileInputStreamOption.readLine())==null){
                                logger.debug("read logMsg and logMsgQueue size is {} fileLinesSize is {}",logMsgQueue.size(),fileLinesSize);
                                logger.debug("current automicInteger is {}",atomicInteger.get());
                                synchronized (atomicInteger){
                                    atomicInteger.addAndGet(1);
                                    if(atomicInteger.get()==fileSize){
                                        atomicInteger.notify();
                                    }
                                }
                                synchronized (fileInputStreamOption){
                                    try {
                                        fileInputStreamOption.wait();
                                    }catch (InterruptedException e){
                                        logger.error(format("%s wait error and file name is %s",Thread.currentThread().getName(),fileName),e);
                                    }
                                }
                                atomicInteger.addAndGet(-1);
                            }
                            logMsgQueue.put(new FileReadModle(fileInputStreamOption.getFileName(),msg,fileInputStreamOption.getOffset()));
                        }
                    }catch (Exception e){
                        logger.error("read "+fileInputStreamOption.getFileName()+" log error",e);
                   }
        }
    }

    public class FileReadModle{
        private String fileName;
        private String msg;
        private long offset;
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FileReadModle that = (FileReadModle) o;

            return fileName.equals(that.fileName);

        }

        @Override
        public int hashCode() {
            return fileName.hashCode();
        }

        @Override
        public String toString() {
            return "FileReadModle{" +
                    "fileName='" + fileName + '\'' +
                    ", offset=" + offset +
                    '}';
        }

        public FileReadModle(String fileName, String msg, long offset) {
            this.fileName = fileName;
            this.msg = msg;
            this.offset = offset;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

    }

    public void close() throws IOException {
        isClose = true;
    }

    public void commit() throws IOException {
        fileReadOffsets.forEach(offset->{
            try {
                wifiinTailFile.get(offset.getFileName()).writeOffset(offset.getOffset());
            } catch (IOException e) {
                logger.error("save offset error:",e);
            }
        });
        isCommit = true;
        events.clear();
    }
}

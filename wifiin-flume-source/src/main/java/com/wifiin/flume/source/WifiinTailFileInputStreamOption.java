package com.wifiin.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.cs.UTF_32;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

/**
 * @auther sunjiayao on 16/5/14.
 * email: sunjiayaocode@gmail.com
 */
public class WifiinTailFileInputStreamOption {
    private static final Logger logger = LoggerFactory.getLogger(WifiinTailFileConfigurationConstants.class);
    private Path filePath;
    private Path offsetPath;
    private long offset = 0;
    private File file;
    private RandomAccessFile fileReader ;
    private RandomAccessFile offsetOutput;
    private String fileName;
    private boolean hasNext = true;
    public WifiinTailFileInputStreamOption(Path filePath,Path offsetPath) throws IOException {
        this.filePath = filePath;
        this.offsetPath = offsetPath;
        fileName = filePath.getFileName().toString();
        init();
    }
    private void init() throws IOException {
        if(!Files.exists(offsetPath)){
            Files.createFile(offsetPath);
            offsetOutput = new RandomAccessFile(offsetPath.toFile(),"rw");
            writeOffset();
        }else{
            offsetOutput = new RandomAccessFile(offsetPath.toFile(),"rw");
        }
        if(!Files.exists(filePath)){
            try {
                Thread.sleep(30*1000);
            } catch (InterruptedException e) {
                logger.error("",e);
            }
        }
        offset = getNativeOffset();
        file = filePath.toFile();
        fileReader = new RandomAccessFile(file,"r");
        fileReader.seek(offset);
    }

    public  void findNewFile() throws IOException {
        logger.debug("find new log file {},logSize is {} offset is {}",fileName,filePath.toFile().length(),offset);
        if(filePath.toFile().length()<offset){
            offset = 0;
            if(fileReader!=null){
                fileReader.close();
            }
            writeOffset();
            init();
        }
    }

    public String readLine() throws IOException, InterruptedException {
        String line;
        if((line=fileReader.readLine())==null){
            findNewFile();
            line=fileReader.readLine();
        }
        logger.debug("thie log ({}) has readLine is {}",fileName,line);
        hasNext = line!=null;
        offset = fileReader.getFilePointer();
        return line;
    }

    private long getNativeOffset() throws IOException {
        return Long.parseLong(new String(Files.readAllBytes(offsetPath)).trim());
    }
    private String generateOffset(long offset){
        StringBuilder resultOffset = new StringBuilder();
        String offsetStr = String.valueOf(offset);
        int index = offsetStr.length();
        while(index++<19){
            resultOffset.append("0");
        }
        return resultOffset.append(String.valueOf(offset)).toString();
    }
    public void writeOffset() throws IOException {
        writeOffset(offset);
    }
    public void writeOffset(long offset) throws IOException {
        offsetOutput.seek(0);
        offsetOutput.write(generateOffset(offset).getBytes());
    }

    public long getOffset() {
        return offset;
    }

    public void close() throws IOException {
        fileReader.close();
        offsetOutput.close();
    }

    public String getFileName() {
        return fileName;
    }

    public boolean isHasNext() {
        return hasNext;
    }
}

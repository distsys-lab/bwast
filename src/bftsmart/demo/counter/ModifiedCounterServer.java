/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.demo.counter;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.ModifiedDefaultSingleRecoverable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Example replica that implements a BFT replicated service (a counter).
 * If the increment > 0 the counter is incremented, otherwise, the counter
 * value is read.
 * 
 * @author alysson
 */

public final class ModifiedCounterServer extends ModifiedDefaultSingleRecoverable {

    private int counter = 0;
    private int checkpointSize;
    private static volatile Logger logger = LoggerFactory.getLogger(ModifiedCounterServer.class);
    private int prev = 0;
    private int reqCount = 0;

    private ModifiedCounterServer(int id, int checkpointSize) {
        this.checkpointSize = checkpointSize;
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final Runnable ticker = () -> {
            logger.info((reqCount - prev) + " reqs/sec");
            prev = reqCount;
        };
        scheduler.scheduleAtFixedRate(ticker, 1, 1, TimeUnit.SECONDS);
    	new ServiceReplica(id, this, this);
    }
            
    @Override
    public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {
        // System.out.println("(" + iterations + ") Counter current value: " + counter);
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(4);
            new DataOutputStream(out).writeInt(counter);
            return out.toByteArray();
        } catch (IOException ex) {
            System.err.println("Invalid request received!");
            return new byte[0];
        }
    }
  
    @Override
    public byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx) {
        try {
            int increment = new DataInputStream(new ByteArrayInputStream(command)).readInt();
            counter += increment;
            reqCount++;
            
            //System.out.println("(" + iterations + ") Counter was incremented. Current value = " + counter);
            
            ByteArrayOutputStream out = new ByteArrayOutputStream(4);
            new DataOutputStream(out).writeInt(counter);
            return out.toByteArray();
        } catch (IOException ex) {
            System.err.println("Invalid request received!");
            return new byte[0];
        }
    }

    public static void main(String[] args){
        if(args.length < 2) {
            System.out.println("Use: java ModifiedCounterServer <processId> <ckpSize: 1KB ~ 1000GB>");
            System.exit(-1);
        }
        logger.info("checkpoint size is " + args[1]);
        new ModifiedCounterServer(Integer.parseInt(args[0]), byteNumWithUnitToInt(args[1]));
    }

    
    @SuppressWarnings("unchecked")
    @Override
    public void installSnapshot(byte[] state) {
        // Install from Huge Snapshot
        try {
            byte[] count = Arrays.copyOfRange(state, 0, 20);
            counter = Integer.parseInt(new String(count));
        } catch (Exception e) {
            System.err.println("[ERROR] Error deserializing state: "
                    + e.getMessage());
        }
    }

    @Override
    public byte[] getSnapshot() {
        // Create Huge Snapshot
        try {
            return createHugeSnapshot(counter, checkpointSize);
        } catch (Exception e) {
            System.err.println("[ERROR] Error serializing state: "
                    + e.getMessage());
            return "ERROR".getBytes();
        }
    }

    private static int byteNumWithUnitToInt(String byteNum) {
        String num = byteNum.substring(0, byteNum.length() - 2);
        String unit = byteNum.substring(byteNum.length() - 2);
        int result = Integer.parseInt(num);
        switch (unit) {
            case "GB":
                result *= 1024;
            case "MB":
                result *= 1024;
            case "KB":
                result *= 1024;
        }
        return result;
    }

    private byte[] createHugeSnapshot(int count, int size) {
        String countString = String.format("%020d", count);
        byte[] snapshot = new byte[size];
        byte[] countBytes = countString.getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(countBytes, 0, snapshot, 0, countBytes.length);
        return snapshot;
    }
}

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.tom.ServiceProxy;

/**
 * Example client that updates a BFT replicated service (a counter).
 * 
 * @author alysson
 */
public class CounterClient {

    private static volatile int count = 0;
    private static volatile int prev = 0;
    private static volatile Logger logger = LoggerFactory.getLogger(CounterClient.class);

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Usage: java ... CounterClient <process id> <increment> [<number of operations>]");
            System.out.println("       if <increment> equals 0 the request will be read-only");
            System.out.println("       default <number of operations> equals 1000");
            System.exit(-1);
        }

        ServiceProxy counterProxy = new ServiceProxy(Integer.parseInt(args[0]));

        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final Runnable ticker = () -> {
            logger.info((count - prev) + " req/s");
            prev = count;
        };
        final ScheduledFuture<?> tickerHandle = scheduler.scheduleAtFixedRate(ticker, 1, 1, TimeUnit.SECONDS);

        try {

            int inc = Integer.parseInt(args[1]);
            int numberOfOps = (args.length > 2) ? Integer.parseInt(args[2]) : 1000;

            for (int i = 0; i < numberOfOps; i++) {

                ByteArrayOutputStream out = new ByteArrayOutputStream(4);
                new DataOutputStream(out).writeInt(inc);
                byte[] reply = (inc == 0)?
                        counterProxy.invokeUnordered(out.toByteArray()):
                	counterProxy.invokeOrdered(out.toByteArray()); //magic happens here
                
                if(reply != null) {
                    int newValue = new DataInputStream(new ByteArrayInputStream(reply)).readInt();
                    if(i == 0) {
                        logger.info("start counting");
                    }
                    //System.out.println(", returned value: " + newValue);
                    count = newValue;
                } else {
                    logger.error("ERROR! Exiting.");
                    break;
                }
                Thread.sleep(10);
            }
        } catch(IOException | NumberFormatException | InterruptedException e){
            counterProxy.close();
        }

        scheduler.schedule(() -> tickerHandle.cancel(true), 0, TimeUnit.SECONDS);
    }
}

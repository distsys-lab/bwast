package bftsmart.demo.map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Console;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class MapNoninteractiveClient {

    private static volatile int count = 0;
    private static int prev = 0;
    private static volatile Logger logger = LoggerFactory.getLogger(MapNoninteractiveClient.class);

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: demo.map.MapNoInteractiveClient <client id> <key size> <value size> <times>");
            return;
        }

        int clientId = Integer.parseInt(args[0]);
        int keySize = Integer.parseInt(args[1]);
        int valueSize = Integer.parseInt(args[2]);
        int times = Integer.parseInt(args[3]);
        // be read from other replica
		//noinspection MismatchedQueryAndUpdateOfCollection
		MapClient<String, String> map = new MapClient<>(clientId);

        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final Runnable ticker = () -> {
            logger.info((count - prev) + " req/s");
        	prev = count;
		};
        final ScheduledFuture<?> tickerHandle = scheduler.scheduleAtFixedRate(ticker, 1, 1, TimeUnit.SECONDS);

        for (int i = 0; i < times; i++) {
            String value = generateRandomString(valueSize);
            for (int j = 0; j < keySize; j++) {
                String key = Integer.toString(i);
                map.put(key, value);
                // be updated from only main thread
				//noinspection NonAtomicOperationOnVolatileField
				count++;
            }
            if(i == 0) {
                logger.info("First puts are done.");
			}
        }
        logger.info("All tasks are done.");
        scheduler.schedule(() -> tickerHandle.cancel(true), 0, TimeUnit.SECONDS);
    }

    private static String generateRandomString(int size) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append((char) (Math.random() * 26) + 'A');
        }
        return sb.toString();
    }
}

package com.example.aeeronsample;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class MediaService {
    static final String channel = "aeron:udp?endpoint=localhost:8081";

    public static void main(String[] args) {
        try (
                MediaDriver mediaDriver = MediaDriver.launch();
                Aeron aeron = Aeron.connect();
        ) {
            System.out.println("Media Driver Active Directory: " + mediaDriver.aeronDirectoryName());

            BalanceProcessor balanceProcessor = new BalanceProcessor(aeron, channel, 1);
            CompletableFuture<BalanceProcessor> balanceProcessorCompletableFuture = balanceProcessor.startSubscribing();
            balanceProcessorCompletableFuture.thenAccept(
                    BalanceProcessor::stop
            );

            CompletableFuture<BalanceUpdaterClient> updater1 =
                    new BalanceUpdaterClient(aeron, channel, 1, 0, 10).startProducing();
            updater1.thenAccept(
                    BalanceUpdaterClient::stop
            );

            CompletableFuture<BalanceUpdaterClient> updater2 =
                    new BalanceUpdaterClient(aeron, channel, 1, 11, 20).startProducing();
            updater2.thenAccept(
                    BalanceUpdaterClient::stop
            );

            CompletableFuture<BalanceUpdaterClient> updater3 =
                    new BalanceUpdaterClient(aeron, channel, 1, 21, 30).startProducing();
            updater3.thenAccept(
                    BalanceUpdaterClient::stop
            );

            while (!updater1.isDone() || !updater2.isDone() || !updater3.isDone()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            balanceProcessor.idBuffer.keySet().forEach(
                    key -> System.out.printf("Buffer value for key: %d is: %d\n", key, balanceProcessor.idBuffer.get(key))
            );
        }

        /*SleepingIdleStrategy sleepingIdleStrategy = new SleepingIdleStrategy();
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(256));
        try (
                MediaDriver driver = MediaDriver.launch();
                Aeron aeron = Aeron.connect();
                Subscription subscription = aeron.addSubscription(channel, 10);
                Publication publication = aeron.addPublication(channel, 10);

        ) {
            System.out.printf("Driver Directory name %s", driver.aeronDirectoryName());

            while (!publication.isConnected()) {
                sleepingIdleStrategy.idle();
            }

            FragmentHandler handler = (buffer1, offset, length, header) -> {
                System.out.println("Received Message: " + buffer1.getInt(offset));
                System.out.println("Session ID:" + header.sessionId());
                map.put(buffer1.getInt(offset), buffer1.getInt(offset));
            };

            while (publication.isConnected()) {
                buffer.putInt(0, new Random().nextInt(100));
                publication.offer(buffer);

                Thread.sleep(1000);

                subscription.poll(handler, 1);
            }

        } catch (Exception e) {
            map.values().forEach(
                    System.out::println
            );

            e.printStackTrace();
        }*/

    }
}

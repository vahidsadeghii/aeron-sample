package com.example.aeeronsample.client;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

public class BalanceUpdaterClient {
    final Publication publication;
    final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(256));
    final int minBound;
    final int maxBound;
    final MediaDriver mediaDriver;
    final Aeron aeron;

    public BalanceUpdaterClient(String channel, int streamId, int minBound, int maxBound) {
        long serviceId = System.currentTimeMillis() + new Random().nextInt(1000);

        mediaDriver = MediaDriver.launchEmbedded(
                new MediaDriver.Context()
                        .threadingMode(ThreadingMode.DEDICATED)
                        .aeronDirectoryName(CommonContext.getAeronDirectoryName())
//                        .aeronDirectoryName(CommonContext.getAeronDirectoryName() + "-" + serviceId + "-client-driver")
                        .dirDeleteOnStart(true)
                        .dirDeleteOnShutdown(true)
        );

        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));

        publication = aeron.addPublication(channel, streamId);
        this.minBound = minBound;
        this.maxBound = maxBound;
    }

    public void stop() {
        publication.close();
        aeron.close();
        mediaDriver.close();
    }

    public CompletableFuture<BalanceUpdaterClient> startProducing() {
        SleepingIdleStrategy sleepingIdleStrategy = new SleepingIdleStrategy();

        return CompletableFuture.supplyAsync(
                () -> {
                    while (!publication.isConnected())
                        sleepingIdleStrategy.idle();

                    int sentCount = 0;
                    Random random = new Random();

                    while (publication.isConnected() && sentCount++ < 1000) {
                        long newValue = random.nextInt(10);
                        buffer.putLong(0, newValue);
                        publication.offer(buffer);
                    }

                    return BalanceUpdaterClient.this;
                }
        );
    }
}

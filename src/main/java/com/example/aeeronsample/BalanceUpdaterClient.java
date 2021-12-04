package com.example.aeeronsample;

import io.aeron.Aeron;
import io.aeron.Publication;
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

    public BalanceUpdaterClient(Aeron aeron, String channel, int streamId, int minBound, int maxBound) {
        publication = aeron.addPublication(channel, streamId);
        this.minBound = minBound;
        this.maxBound = maxBound;
    }

    public void stop() {
        publication.close();
    }

    CompletableFuture<BalanceUpdaterClient> startProducing() {
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

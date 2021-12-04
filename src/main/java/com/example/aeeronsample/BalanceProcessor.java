package com.example.aeeronsample;

import io.aeron.Aeron;
import io.aeron.Subscription;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.SleepingIdleStrategy;

import java.util.concurrent.CompletableFuture;

public class BalanceProcessor {
    final Subscription subscription;
    final Long2LongHashMap idBuffer = new Long2LongHashMap(-1);

    public BalanceProcessor(Aeron aeron, String channel, int streamId) {
        subscription = aeron.addSubscription(channel, streamId);
    }

    public void stop() {
        subscription.close();
    }

    public CompletableFuture<BalanceProcessor> startSubscribing() {
        return CompletableFuture.supplyAsync(
                () -> {
                    SleepingIdleStrategy sleepingIdleStrategy = new SleepingIdleStrategy();
                    while (!subscription.isConnected())
                        sleepingIdleStrategy.idle();

                    while (subscription.isConnected())
                        subscription.poll(
                                (buffer, offset, length, header) -> {
                                    long fetchItemKey = buffer.getLong(offset);
                                    long bufferValue = idBuffer.get(fetchItemKey);
                                    idBuffer.put(fetchItemKey,
                                            bufferValue == -1 ? 1L : bufferValue + 1);

                                }, 1
                        );

                    return BalanceProcessor.this;
                }
        );
    }
}

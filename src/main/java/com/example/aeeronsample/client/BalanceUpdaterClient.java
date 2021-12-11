package com.example.aeeronsample.client;

import baseline.BalanceEncoder;
import baseline.MessageHeaderEncoder;
import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class BalanceUpdaterClient {
    final Publication publication;
    final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(256));
    final List<Integer> randomValue;
    final MediaDriver mediaDriver;
    final Aeron aeron;
    final MessageHeaderEncoder messageHeaderEncoder;

    public final static AtomicInteger counter = new AtomicInteger(0);

    public BalanceUpdaterClient(String channel, int streamId, List<Integer> randomValue) {
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
        this.randomValue = randomValue;
        messageHeaderEncoder = new MessageHeaderEncoder();

    }

    public void stop() {
        publication.close();
        aeron.close();
        mediaDriver.close();
    }

    public CompletableFuture<BalanceUpdaterClient> startProducing() {
        counter.incrementAndGet();

        SleepingIdleStrategy sleepingIdleStrategy = new SleepingIdleStrategy();

        return CompletableFuture.supplyAsync(
                () -> {
                    while (!publication.isConnected())
                        sleepingIdleStrategy.idle();

                    BalanceEncoder balanceEncoder = new BalanceEncoder();
                    balanceEncoder.wrapAndApplyHeader(
                            buffer, 0, messageHeaderEncoder
                    );

                    randomValue.forEach(
                            value -> {

                                balanceEncoder.amount(value + 1000);
                                balanceEncoder.userId(value);

                                publication.offer(buffer);
                            }
                    );

                    counter.decrementAndGet();
                    return BalanceUpdaterClient.this;
                }
        );
    }
}

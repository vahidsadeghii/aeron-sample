package com.example.aeeronsample;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.Random;

public class MediaService {
    static final String channel = "aeron:udp?endpoint=localhost:8081";

    public static void main(String[] args) {
        SleepingIdleStrategy sleepingIdleStrategy = new SleepingIdleStrategy();
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(256));
        try (
                MediaDriver driver = MediaDriver.launch();
                Aeron aeron = Aeron.connect();
                Subscription subscription = aeron.addSubscription(channel, 10);
                Publication publication = aeron.addPublication(channel, 10);

        ) {
            while (!publication.isConnected()) {
                sleepingIdleStrategy.idle();
            }

            FragmentHandler handler = (buffer1, offset, length, header) -> {
                System.out.println("Received Message: " + buffer1.getInt(offset));
                System.out.println("Session ID:" + header.sessionId());
            };

            while (publication.isConnected()) {
                buffer.putInt(0, new Random().nextInt(100));
                publication.offer(buffer);

                Thread.sleep(1000);

                subscription.poll(handler, 1);
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}

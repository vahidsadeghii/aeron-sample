package com.example.aeeronsample.server;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SleepingIdleStrategy;

import java.util.Random;

public class BalanceProcessor {
    static long startTime = 0;
    static long endTime = 0;

    public BalanceProcessor(String channel) {

        MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED) //Why SHARED?
                .aeronDirectoryName(CommonContext.getAeronDirectoryName())
                .aeronDirectoryName(CommonContext.getAeronDirectoryName() + "-" +
                        System.currentTimeMillis() + new Random().nextInt(1000)
                        + "-server")
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaDriverContext);
        System.out.println("Media Driver Started");


        Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));

        ServerAgent agent = new ServerAgent(aeron, channel);
        AgentRunner agentRunner = new AgentRunner(new SleepingIdleStrategy(), Throwable::printStackTrace, null, agent);

        AgentRunner.startOnThread(agentRunner);

        new ShutdownSignalBarrier().await();

        CloseHelper.quietClose(agentRunner);
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(mediaDriver);
    }


    public static class ServerAgent implements Agent {
        private final String channel;
        private Subscription subscription;
        private final Aeron aeron;
        private final Poller poller;


        public ServerAgent(Aeron aeron, String channel) {
            this.aeron = aeron;
            this.channel = channel;
            this.poller = new Poller();
        }

        @Override
        public void onStart() {
            subscription = aeron.addSubscription(channel, 1);
            System.out.println("Start Consuming...");

        }

        @Override
        public int doWork() {
            int pollResult = subscription.poll(poller, 1);

            if (pollResult <= 0 && startTime > 0 && endTime == 0) {
                endTime = System.currentTimeMillis();
                System.out.println("Duration:" + (endTime - startTime));
            }



            return pollResult;
        }

        @Override
        public String roleName() {
            return "Server";
        }

        @Override
        public void onClose() {
            poller.closePublication();
            subscription.close();
        }
    }

    public static class Poller implements FragmentHandler {
        final Long2LongHashMap idBuffer = new Long2LongHashMap(-1);


        @Override
        public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
            if (startTime == 0) {
                startTime = System.currentTimeMillis();

            }

            long fetchItemKey = buffer.getLong(offset);
            long bufferValue = idBuffer.get(fetchItemKey);
            idBuffer.put(fetchItemKey,
                    bufferValue == -1 ? 1L : bufferValue + 1);
            System.out.println("Fetch Value: " + fetchItemKey);
        }

        public void closePublication() {
            idBuffer.values().forEach(
                    value -> System.out.println("Buffer Value:" + value)
            );
        }
    }
}

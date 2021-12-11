package com.example.aeeronsample.server;

import baseline.BalanceDecoder;
import baseline.MessageHeaderDecoder;
import com.example.aeeronsample.domain.Balance;
import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SleepingIdleStrategy;

import java.util.Random;

public class BalanceProcessor {
    public static long startTime = 0;
    public static long endTime = 0;
    public static Long2ObjectHashMap<Balance> idBuffer = new Long2ObjectHashMap<>();

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
            int poll = subscription.poll(poller, 1);

            if (poll <= 0 && startTime > 0 && endTime == 0)
                endTime = System.currentTimeMillis();

            return poll;
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
        private final BalanceDecoder balanceDecoder = new BalanceDecoder();
        private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

        @Override
        public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
            if (startTime == 0) {
                startTime = System.currentTimeMillis();

            }

            balanceDecoder.wrap(buffer, offset + headerDecoder.encodedLength(),
                    headerDecoder.blockLength(), headerDecoder.version());

            Balance balance = idBuffer.getOrDefault(balanceDecoder.userId(),
                    new Balance(balanceDecoder.userId(), 0));

            balance.setAmount(balance.getAmount() + balanceDecoder.amount());
        }

        public void closePublication() {
            idBuffer.values().forEach(
                    value -> System.out.println("Buffer Value:" + value)
            );
        }
    }
}

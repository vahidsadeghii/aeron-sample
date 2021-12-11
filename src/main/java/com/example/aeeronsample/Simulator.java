package com.example.aeeronsample;

import com.example.aeeronsample.client.BalanceUpdaterClient;
import com.example.aeeronsample.server.BalanceProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

public class Simulator {
    public static void main(String[] args) {
        new Thread(
                () -> new BalanceProcessor("aeron:udp?endpoint=localhost:8081")
        ).start();


        Random random = new Random();
        List<Integer> randomValues = new ArrayList<>();

        IntStream.range(
                0, 1000
        ).forEach(
                value -> randomValues.add(random.nextInt(10))
        );

        IntStream.range(0, 10).parallel().forEach(
                value -> new BalanceUpdaterClient("aeron:udp?endpoint=localhost:8081", 1, randomValues)
                        .startProducing()
        );

        while (BalanceUpdaterClient.counter.get() > 0) {
            try {
                Thread.sleep(5000);
                System.out.println("Counter Value: " + BalanceUpdaterClient.counter.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("End Time: " + BalanceProcessor.endTime);
        System.out.println("Start Time: " + BalanceProcessor.startTime);
        System.out.println("Duration: " + (BalanceProcessor.endTime - BalanceProcessor.startTime));

        BalanceProcessor.idBuffer.forEach(
                (key, value) -> System.out.printf("Buffer Value for UseId:%d and Value:%d \n", key, value.getAmount())
        );
    }
}

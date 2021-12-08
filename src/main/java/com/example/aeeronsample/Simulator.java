package com.example.aeeronsample;

import com.example.aeeronsample.client.BalanceUpdaterClient;
import com.example.aeeronsample.server.BalanceProcessor;

import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

public class Simulator {
    public static void main(String[] args) {
        new Thread(
                () -> new BalanceProcessor("aeron:udp?endpoint=localhost:8081")
        ).start();


        IntStream.range(0, 10).parallel().forEach(
                value -> new BalanceUpdaterClient("aeron:udp?endpoint=localhost:8081", 1, 0, 1000).startProducing()
        );
    }
}

package com.example.aeeronsample.domain;

public class Balance {
    private long userId;
    private long amount;

    public Balance(long userId, long amount) {
        this.userId = userId;
        this.amount = amount;
    }

    public long getUserId() {
        return userId;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }
}

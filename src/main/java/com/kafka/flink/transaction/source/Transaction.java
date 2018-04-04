package com.kafka.flink.transaction.source;

public class Transaction {

    public enum TransactionType { PAYMENT, CREDIT, AUTHORIZATON };

    private String uuid;
    private TransactionType transactionType;

    private long value;
    private String account;
    private long timestamp;

    public Transaction() {
        // Default Constructor
    }

    public Transaction(String uuid, TransactionType transactionType, long value, String account, long timestamp) {
        this.uuid = uuid;
        this.transactionType = transactionType;
        this.value = value;
        this.account = account;
        this.timestamp = timestamp;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(TransactionType transactionType) {
        this.transactionType = transactionType;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Transaction {" +
                "uuid='" + uuid + '\'' +
                ", transactionType=" + transactionType +
                ", value=" + value +
                ", account='" + account + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}

package com.kafka.flink.transaction.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Custom source generating <code>Transaction</code> entries/POJOs
 */
public class TransactionGenerator implements SourceFunction<Transaction> {
    private static final long serialVersionUID = 2174904787118597072L;
    private boolean running = true;
    private int accountRange;
    private long interval;

    private final static int DEFAULT_ACCOUNT_RANGE = 1000;
    private final static long DEFAULT_INTERVAL = 1000;

    public TransactionGenerator() {
        this(DEFAULT_ACCOUNT_RANGE, DEFAULT_INTERVAL);
    }

    public TransactionGenerator(int accountRange, long interval) {
        this.accountRange = accountRange;
        this.interval = interval;
    }

    @Override
    public void run(SourceContext<Transaction> sourceContext) throws Exception {
        while(this.running) {
            Transaction transaction = generateTransaction();
            System.out.printf("\nGenerating: %s", transaction.toString());

            sourceContext.collect(transaction);
            Thread.sleep(this.interval);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

    private Transaction generateTransaction() {

        final String uuid = UUID.randomUUID().toString();
        final Transaction.TransactionType transactionType = getRandomTransactionType();
        final long randomValue = ThreadLocalRandom.current().nextLong(-1000, 1001);
        int randomAccountNumber = ThreadLocalRandom.current().nextInt(1, accountRange + 1);
        return new Transaction(uuid, transactionType, randomValue, Integer.toString(randomAccountNumber), System.currentTimeMillis());
    }

    private Transaction.TransactionType getRandomTransactionType() {
        final Transaction.TransactionType[] transactionTypes = Transaction.TransactionType.values();
        int transactionTypeIndex = ThreadLocalRandom.current().nextInt(0, transactionTypes.length);

        return transactionTypes[transactionTypeIndex];
    }
}

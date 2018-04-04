package com.kafka.flink.transaction.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import com.google.gson.Gson;

/**
 * Custom SerializationSchema/DeserializationSchema for handling <code>Transaction</code> records.
 * <p>
 *     Ideally these would not be serialized to-and-from JSON in a PROD streaming environment,
 *     but this was done for this example for debugging at the schema level.
 * </p>
 */
public class TransactionSchema implements DeserializationSchema<Transaction>, SerializationSchema<Transaction> {

    @Override
    public Transaction deserialize(byte[] bytes) {
        final Gson gson = new Gson();
        final String transactionJSON = new String(bytes);
        return gson.fromJson(transactionJSON, Transaction.class);
    }

    @Override
    public boolean isEndOfStream(Transaction transaction) {
        return false;
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeExtractor.getForClass(Transaction.class);
    }

    @Override
    public byte[] serialize(Transaction transaction) {
        final Gson gson = new Gson();
        return gson.toJson(transaction).getBytes();
    }
}

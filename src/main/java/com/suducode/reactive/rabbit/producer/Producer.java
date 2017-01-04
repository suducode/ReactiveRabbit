package com.suducode.reactive.rabbit.producer;

/**
 * Producer Contract.
 *
 * @author Sudharshan Krishnamurthy
 * @version 1.0
 */
public interface Producer<T> {

    void close();

    String getId();

    boolean push(T data);
}

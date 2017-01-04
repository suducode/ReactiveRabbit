package com.suducode.reactive.rabbit.consumer;

import java.util.concurrent.TimeUnit;

/**
 * Consumer Contract.
 *
 * @author Sudharshan Krishnamurthy
 * @version 1.0
 */
public interface Consumer<T> {

    boolean hasNext();

    T poll(int timeout, TimeUnit unit);

    void close();

    String getId();
}

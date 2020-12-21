package com.eventstore.dbclient;

@FunctionalInterface
public interface ThrowingBiFunction<TFirst, TSecond, TResult, TException extends Throwable> {

    TResult apply(TFirst first, TSecond second) throws TException;
}

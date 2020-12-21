package com.eventstore.dbclient;

@FunctionalInterface
public interface ThrowingFunction<TInput, TResult, TException extends Throwable> {

    TResult apply(TInput first) throws TException;
}

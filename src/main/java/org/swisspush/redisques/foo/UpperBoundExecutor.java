package org.swisspush.redisques.foo;

import io.vertx.core.Future;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;


public interface UpperBoundExecutor extends Executor {

    <T> Future<T> execute(Callable<T> tasks);

}

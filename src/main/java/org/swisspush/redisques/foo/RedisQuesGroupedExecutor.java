package org.swisspush.redisques.foo;

import io.vertx.core.Future;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;


/**
 * See {@link #executeUntilFail(Iterator)}.
 */
public interface RedisQuesGroupedExecutor {

    /**
     * returns a regular {@link Executor}, but backed by this implementation.
     * Eg the returned executor MAY be constrained by similar scheduling
     * implications.
     */
    Executor asExecutor();

    /**
     * Asynchronously executes all of the given `tasks` and completes returned
     * future as soon all tasks have been executed. As soon as a task fails,
     * later tasks won't be executed anymore and returned future will be
     * failed, with the results evaluated so far.
     */
    <T> Future<Collection<Future<T>>> executeUntilFail(Iterator<Callable<Future<T>>> tasks);

    /**
     * Asynchronously executes all of the given `tasks` and completes returned
     * future as soon all tasks have been executed. If any of the tasks
     * fails, execution of the remaining tasks will continue.
     */
    <T> Future<Collection<Future<T>>> executeDespiteFail(Iterator<Callable<Future<T>>> tasks);

}

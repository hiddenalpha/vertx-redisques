package org.swisspush.redisques.foo;

import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.Iterator;
import java.util.concurrent.Callable;


class UpperBoundExecutorImpl /*implements UpperBoundExecutor*/ {

//    private final Vertx vertx;
//    private final int limit;
//
//    UpperBoundExecutorImpl(Vertx vertx, int limit) {
//        this.vertx = vertx;
//        this.limit = limit;
//    }
//
//    @Override
//    public void execute(Runnable task) {
//        execute(new Iterator<Callable<Void>>() {
//            Runnable elem = task;
//            @Override public boolean hasNext() { return elem != null; }
//            @Override public Callable<Void> next() {
//                var tmp = elem;
//                elem = null;
//                return FooDewohaew.callableOf(tmp);
//            }
//        });
//    }
//
//    @Override
//    public <T> Future<Void> execute(Iterator<Callable<T>> tasks) {
//        assert false : "TODO";
//        return null;
//    }

}

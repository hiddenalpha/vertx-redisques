package org.swisspush.redisques.foo;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static org.slf4j.LoggerFactory.getLogger;
import static org.swisspush.redisques.foo.FooDewohaew.onClose;
import static org.swisspush.redisques.foo.FooDewohaew.onSend;


class RedisApiInterceptor {

    private static final Logger log = getLogger(RedisApiInterceptor.class);

    static class Ctx implements RedisAPI {
        static final AtomicReference<Field> commandName = new AtomicReference<>();
        final Logger log = RedisApiInterceptor.log;
        final ReentrantLock mutx = new ReentrantLock();
        int metricIntervalSec;
        Consumer<RedisAccessMetric> metricObserver;
        Vertx vertx;
        RedisAPI delegate;
        HashMap<String, PerCmd> currentMetricsByCmd = new HashMap<>();
        long metricPeriodBeginNs = System.nanoTime();
        long metricTimerId;
        long numRedisCalls;

        @Override public Future<Response> send(Command cmd, String... args) { return onSend(this, cmd, args); }
        @Override public void close() { onClose(this); }
    }


    static class PerCmd {
        public long numCalled = 0;
    }


    static interface RedisAccessMetric {
        /**
         * Duration of this measuring period in seconds.
         */
        float getSampleDurationSeconds();

        /**
         * Number of redis calls (no matter what command exactly) made during this measurement period.
         */
        long getNumRedisCallsTotal();

        /**
         * More fine-grained details about specific commands. The returned map
         * MAY only contain entries for commands, that EFFECTIVELY did occur
         * during this measure period.
         */
        Map<String, RedisAccessMetricPerCmd> getMetricPerCmd();
    }


    static interface RedisAccessMetricPerCmd {
        /**
         * How this redis command is named. For example is the value used as key
         * in {@link RedisAccessMetric#getMetricPerCmd()}.
         */
        String getCmdName();

        /**
         * How many times this command got requested to execute.
         */
        long getTimesCalled();
    }


}

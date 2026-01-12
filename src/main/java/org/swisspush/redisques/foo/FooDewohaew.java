package org.swisspush.redisques.foo;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.impl.CommandImpl;
import org.slf4j.Logger;
import org.swisspush.redisques.foo.RedisApiInterceptor.Ctx;
import org.swisspush.redisques.foo.RedisApiInterceptor.PerCmd;
import org.swisspush.redisques.foo.RedisApiInterceptor.RedisAccessMetric;
import org.swisspush.redisques.foo.RedisApiInterceptor.RedisAccessMetricPerCmd;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;


public interface FooDewohaew {


    public static <T> java.util.concurrent.Callable<T> callableOf(java.lang.Runnable r) {
        return () -> {
            r.run();
            return null;
        };
    }


    public static RedisAPI redisApiDecorate(Vertx vertx, @Nullable RedisAPI delegate) {
        var that = new Ctx();
        that.vertx = vertx;
        that.delegate = delegate;
        return that;
    }


    public static void setRedisApi(RedisAPI base, RedisAPI redisApi) {
        assert base instanceof Ctx : "base instanceof Ctx";
        assert redisApi != null : "redisApi != null";
        Ctx that = (Ctx) base;
        that.delegate = redisApi;
    }


    static Future<Response> onSend(Ctx that, Command cmd, String[] args) {
        if (that.delegate == null)
            return Future.failedFuture("MUST NOT use that instance until delegate got provided by calling `setRedisApi()`!");
        String cmdName = commandNameGet(that, cmd);
        that.mutx.lock();
        try {
            that.numRedisCalls += 1;
            PerCmd cmdCtx = that.currentMetricsByCmd.get(cmdName);
            cmdCtx.numCalled += 1;
        } finally {
            that.mutx.unlock();
        }
        return that.delegate.send(cmd, args);
    }


    static void onClose(Ctx that) {
        if (that.delegate != null) {
            that.delegate.close();
        }
        setMetricsObserver(that, 15, null);
    }


    static String commandNameGet(Ctx that, Command cmd) {
        Field field = that.commandName.updateAndGet(oldVal -> {
            if (oldVal != null) return oldVal;
            try {
                Field f = CommandImpl.class.getDeclaredField("command");
                f.setAccessible(true);
                return f;
            } catch (NoSuchFieldException e) {
                throw new UnsupportedOperationException("TODO", e);
            }
        });
        try {
            return (String) field.get(cmd);
        } catch (IllegalAccessException e) {
            throw new UnsupportedOperationException("TODO", e);
        }
    }


    static void setMetricsObserver(Ctx that, int intervalSec, Consumer<RedisAccessMetric> metricsObserver) {
        if (intervalSec <= 0) throw new IllegalArgumentException(String.valueOf(intervalSec));
        Consumer<RedisAccessMetric> oldInstance;

        that.mutx.lock();
        try {
            oldInstance = that.metricObserver;
            that.metricObserver = metricsObserver;
            that.metricIntervalSec = intervalSec;
            that.metricTimerId = that.vertx.setTimer(that.metricIntervalSec * 1000L, t -> onMetricTimerExpired(that, t));
        } finally {
            that.mutx.unlock();
        }

        if (oldInstance != null) {
            Logger log = getLoggerFor(oldInstance);
            log.warn("This instance is NO LONGER the metricsObserver! Someone replaced it by: {}", metricsObserver);
        }
    }


    private static void onMetricTimerExpired(Ctx that, Long myTimerId) {
        long metricTimerId;
        long numRedisCalls;
        int metricIntervalSec;
        Consumer<RedisAccessMetric> metricObserver;
        HashMap<String, PerCmd> currentMetricsByCmd;
        long metricPeriodEndNs, metricPeriodBeginNs;

        that.mutx.lock();
        try {
            metricPeriodEndNs = System.nanoTime();
            /* get out the required data */
            metricTimerId = that.metricTimerId;
            metricObserver = that.metricObserver;
            metricIntervalSec = that.metricIntervalSec;
            numRedisCalls = that.numRedisCalls;
            currentMetricsByCmd = that.currentMetricsByCmd;
            metricPeriodBeginNs = that.metricPeriodBeginNs;
            /* update state where needed, to prepare next measuring period */
            that.metricPeriodBeginNs = metricPeriodEndNs;
            that.currentMetricsByCmd = new HashMap<>(32);
        } finally {
            that.mutx.unlock();
        }

        /* MUST NOT do anything. another observer (or a null-observer) has
         * replaced us meanwhile */
        if (myTimerId != metricTimerId || metricObserver == null) return;

        try {
            /* setup metrics for publication */
            float durationSec = nanosSmallDiff(metricPeriodEndNs, metricPeriodBeginNs) / 1_000_000_000.f;
            Map<String, RedisAccessMetricPerCmd> metricsPerCmd = new HashMap<>(currentMetricsByCmd.size());
            for (Map.Entry<String, PerCmd> e : currentMetricsByCmd.entrySet()) {
                String cmdName = e.getKey();
                PerCmd perCmd = e.getValue();
                long numCalled = perCmd.numCalled;
                metricsPerCmd.put(cmdName, new RedisAccessMetricPerCmd() {
                    @Override public String getCmdName(){ return cmdName; }
                    @Override public long getTimesCalled(){ return numCalled; }
                });
            }
            var metrics = new RedisAccessMetric() {
                @Override public float getSampleDurationSeconds() { return durationSec; }
                @Override public long getNumRedisCallsTotal() { return numRedisCalls; }
                @Override public Map<String, RedisAccessMetricPerCmd> getMetricPerCmd() { return metricsPerCmd; }
            };

            /* publish */
            try {
                if (metricObserver != null) metricObserver.accept(metrics);
            } catch (Exception ex) {
                var log = getLoggerFor(metricObserver);
                log.error("{}", ex.getMessage(), log.isDebugEnabled() ? ex : null);
            }
        } catch (Exception ex) {
            that.log.warn("{}", ex, that.log.isDebugEnabled() ? ex : null);
        }
        /* schedule next run */
        that.vertx.setTimer(metricIntervalSec * 1000L, t -> onMetricTimerExpired(that, t));
    }


    public static int clamp(int val, int min, int max) {
        return val < min ? min : val > max ? max : val;
    }


    public static org.slf4j.Logger getLoggerFor(Object obj) {
        String nm = obj.getClass().getName();
        int pos = nm.indexOf('$');
        if (pos != -1) nm = nm.substring(0, pos);
        return org.slf4j.LoggerFactory.getLogger(nm);
    }


    /**
     * Find smallest distance assuming integers overflow "like a circle".
     *
     * Computers cannot represent all existing integers. Due to how
     * integers are represented in java, they are not infinite but
     * more like a circle. Speak when we infinitely increment an integer,
     * it overflows and (usually) continues to walk around this
     * (imaginary) circle.
     *
     * This function takes two of those numbers on this circle and
     * returns the smallest distance to travel on the circle between
     * them. Here some examples:
     * <ul>
     *   <li>f(7, 13) = 6</li>
     *   <li>f(-7, +11) = 18</li>
     *   <li>f(LONG_MIN, LONG_MAX) = 1</li>
     *   <li>f(-9223372036854775805, 9223372036854775802) = 9</li>
     * </ul>
     *
     * This can be handy for example in conjunction with {@link System#nanoTime()}.
     * Because in case of overflows between measuring begNs and endNs a
     * simple subtraction would lead to uselessly large results. So we
     * can use it as:
     * <code>
     *   long begNs = System.nanoTime();
     *   long endNs = System.nanoTime();
     *   long durationNs = nanosSmallDiff(endNs, begNs);
     * </code>
     *
     * WARN: Do NOT use this if your distance can reach (LONG_MAX / 2).
     * Because in this case you would get wrong (too small) results.
     *
     * <a href="https://git.hiddenalpha.ch/UnspecifiedGarbage.git/tree/src/main/java/ch/hiddenalpha/unspecifiedgarbage/time/TimeUtils.java">Source</a>.
     *
     */
    public static long nanosSmallDiff( long a, long b ){
        return (a - b >= 0) ? a - b : b - a;
    }


}

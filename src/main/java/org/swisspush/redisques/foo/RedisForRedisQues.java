package org.swisspush.redisques.foo;

import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;

import javax.annotation.Nullable;
import java.util.List;


/**
 * Service wrapper around {@link io.vertx.redis.client.RedisAPI} providing
 * convenience methods for commonly used Redis commands in RedisQues.
 *
 * <p>
 * All methods return Vert.x {@link Future}s and are non-blocking.
 * </p>
 *
 * (Comment CopyPasta from obsolete {@link org.swisspush.redisques.queue.RedisService})
 */
public interface RedisForRedisQues extends RedisAPI {

    /**
     * Redis script command.
     *
     * @param name  Lua script command
     * @param value value pass in
     * @return a {@link Future} containing the SHA1 hash of the loaded script
     */
    Future<Response> script(String name, String value);

    /**
     * Sets a key only if it does not already exist, with a TTL.
     *
     * @param key            Redis key
     * @param value          value to store
     * @param setNx          add NX flag
     * @param durationMillis expiration duration
     * @return a {@link Future} indicating whether the key was set
     */
    Future<Boolean> setNxPx(String key, String value, boolean setNx, long durationMillis);

    /**
     * Adds or updates a member in a sorted set.
     *
     * @param key   sorted set key
     * @param score member score
     * @param value member value
     * @return a {@link Future} containing the number of added elements
     */
    Future<Response> zadd(String key, String value, String score);

    /**
     * Adds or updates a member in a sorted set.
     *
     * @param key    sorted set key
     * @param params addtional flag list
     * @param score  member score
     * @param value  member value
     * @return a {@link Future} containing the number of added elements
     */
    Future<Response> zadd(String key, List<String> params, String value, String score);

    /**
     * Removes a member from a sorted set.
     *
     * @param key   the key of the sorted set
     * @param value the member to be removed from the sorted set
     * @return a {@link Future} that completes with the Redis {@link Response}
     * indicating the number of removed elements
     */
    Future<Response> zrem(String key, String value);

    /**
     * Sets the value of a field in a hash.
     *
     * @param queuesKey the key of the hash
     * @param queueName the field name within the hash
     * @param value     the value to set for the specified field
     * @return a {@link Future} that completes with the Redis {@link Response}
     * indicating whether the field was newly added or updated
     */
    Future<Response> hset(String queuesKey, String queueName, String value);

    /**
     * Removes a field from a hash.
     *
     * @param key   Redis hash key
     * @param field field to remove
     * @return a {@link Future} containing number of removed fields
     */
    Future<Response> hdel(String key, String field);

    /**
     * Returns sorted set members with scores within a range.
     *
     * @param key Redis sorted set key
     * @param min minimum score
     * @param max maximum score
     * @return a {@link Future} containing the matching members
     */
    Future<Response> zrangebyscore(String key, String min, String max);

    /**
     * Appends one or more values to a list.
     *
     * @param key  Redis list key
     * @param item values to append
     * @return a {@link Future} containing the new list length
     */
    Future<Response> rpush(String key, String item);

    /**
     * Sets a timeout on a key.
     *
     * @param key     Redis key
     * @param seconds time to live
     * @return a {@link Future} indicating if the timeout was set
     */
    Future<Response> expire(String key, String seconds);

    /**
     * Iterates over keys in the database incrementally.
     *
     * @param cursor  scan cursor
     * @param pattern optional match pattern
     * @param count   optional batch size
     * @param type    optional key type filter
     * @return a {@link Future} containing scan result
     */
    Future<Response> scan(String cursor, @Nullable String pattern, @Nullable String count, @Nullable String type);

}

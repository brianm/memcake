[![Maven Central](https://img.shields.io/maven-central/v/org.skife.memcake/memcake.svg)](https://search.maven.org/#search%7Cga%7C1%7Ca%3Amemcake%20g%3Aorg.skife.memcake)

# Memcake

Memcake is a Java client for [Memcached](https://memcached.org/). It speaks only the [binary protocol](https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped), and provides relatively low level access to memcached primitives. Basically, each command memcached exposes via the binary protocol is exposed 1:1 in memcake.

# Concurrency

All operations occur asynchronously on the thread pool for the [`AsynchronousChannelGroup`](https://docs.oracle.com/javase/8/docs/api/java/nio/channels/AsynchronousChannelGroup.html) that the connection to the server is using. Results are made available via [`CompletableFuture`](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html). Any callbacks attached to the future should not do much work on the thread they are called on, but should pass off blocking operations of large calculations to alternate threads.

Memcake requires Java 1.8, and has no other runtime dependencies.

# Limitations

## One Server

As of the currently released version, Memcake only supports communicating with a single memcached server instance per client. The near-term plan is to support [libmemcached](http://libmemcached.org/libMemcached.html) compatible sharding in Memcake, but it has not been finished yet.

## Evented IO and OS X

Memcake uses evented IO via NIO. This works well on operating systems with good evented IO, such as Linux, FreeBSD, and Windows. OS X, on the other hand, has a seemingly crippled kqueue implementation and Java does not play nicely with it, so you can see very high levels of kernel CPU consumption using this library even moderately under high load (such as running the tests).

# Usage

## Maven Dependency Block

You can find the latest release in [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22memcake%22).

```xml
<dependency>
    <groupId>org.skife.memcake</groupId>
    <artifactId>memcake</artifactId>
    <version>${memcake.version}</version>
</dependency>
```

## Creating a Client

The simplest way to create a client is just to pass the server to connect to (currently, it only supports one), the max number of in-flight requests to allow (to avoid unbounded queues), and a default timeout:

```java
Memcake mc = Memcake.create(memcached.getAddress(), // memcached address
                            1000,                   // max concurrent requests
                            Duration.ofSeconds(1)); // default operation timeout
```

If finer grained control is needed over the network configuration, thread pool sizes, etc, you can alternately construct a client providing all of these things:

```java
ScheduledExecutorService timer = Executors.newScheduledThreadPool(1);
ExecutorService pool = Executors.newFixedThreadPool(4);
AsynchronousChannelGroup group = AsynchronousChannelGroup.withThreadPool(pool);

Memcake mc = Memcake.create(Collections.singleton(memcached.getAddress()),
                            Duration.ofSeconds(1),
                            (addr) -> {
                                try {
                                    AsynchronousSocketChannel chan = AsynchronousSocketChannel.open(group);
                                    return Connection.open(addr, 1000, chan, timer);
                                } catch (IOException e) {
                                    throw new IllegalStateException(e);
                                }
                            });
```

In this, more general, form of client creation you pass in the set of servers it should connect to (which must have a size of one right now), the default duration, and a function which creates an `Connection` object from the address. A `Connection` represents the low level connection to the memcached server, and needs to receive the address, maximum in flight request count, an `AsynchronousSocketChannel` to perform the IO on, and a scheduled executor which is used to trigger timeouts.

The socket channel MUST NOT be connected when it is passed in, but can be otherwise configured as desired.

## Using a Client

Operations on `Memcake` match 1:1 with the [memcached binary protocol](https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped), so any questions about behavior can be looked up there. The required argumnents for operations are parameters on the methods on `Memcake`, which return a builder that can receive optional arguments, and be used to execute the operation:

```java
Memcake mc = Memcake.create(memcached.getAddress(),
                            1000,
                            Duration.ofSeconds(1));

CompletableFuture<Version> fv = mc.set("hello", "world")
                                    .expires(300)
                                    .flags(0xCAFE)
                                    .execute();

Version v1 = fv.get();

CompletableFuture<Optional<Value>> f = mc.get("hello")
                                            .execute();

Optional<Value> ov = f.get();

assertThat(ov).isPresent();
ov.ifPresent((v) -> {
    assertThat(v.getVersion()).isEqualTo(v1);
    assertThat(v.getFlags()).isEqualTo(0xCAFE);
    assertThat(v.getValue()).isEqualTo("world".getBytes(StandardCharsets.UTF_8));
});

mc.close();
```

This example issues a `set` and then `get` for a value. Keys and values can generally be `String` or `byte[]`. If a `String` is used, as is the case here, it will be converted to a UTF-8 encoded `byte[]`.

Set provides a `Value` which can be optionally used as a compare and swap (cas) value on many operations. Get provides a `Value` which makes available all the information memcached returns, generally the version (cas), the actual bytes value, and flags. The [`getk`](https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped#get-get-quietly-get-key-get-key-quietly) variant operations also make the key available on the value.

## Quiet Operations

Many operations on memcached support a "quiet" variant, where the server only responds if the response is "interesting." You can see the [binary protocol spec](https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped) for details on what qualifies as interesting for what operations. In Memcake, if the server chooses to NOT send a response, the `CompletableFuture` from the client will not complete until a non-quiet operation sent *after* the quiet operation completes, or the operation timeout is reached (in which case the future will complete exceptionally with a timeout). If you are sending a series of quiet operations, you should generally send the last operation non-quietly, or send a `noop()`, in order to have those futures complete normally and know that they succeeded (or failed, in the case of `get(...)`).

## MultiGet and Friends

Memcached's binary protocol (and Memcake at this time) has no first class concept of multiget! Multiget is implemented via a series of `getq(...)` operations followed by a final `get(...)` operation. You can think of this as a batch operation which optimizes wire transfers. The nice part about this is that you can mix in any combination of operations you like -- increments, sets, gets, etc into a generalized "multi-op" instead of just a multiget. It is important to send that last operation non quietly, or send a noop, though, so you can know when the whole batch completes, and force the `Optional.empty()` result for the getqs.

# License

Licensed under [Apache License 2.0](https://github.com/brianm/memcake/blob/master/LICENSE). No runtime dependencies outside the standard library. 

package org.projectreactor.bench.reactor;

import org.openjdk.jmh.annotations.*;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.bus.Event;
import reactor.core.dispatch.RingBufferDispatcher;
import reactor.core.dispatch.RingBufferDispatcher3;
import reactor.core.processor.RingBufferProcessor;
import reactor.fn.Consumer;
import reactor.jarjar.com.lmax.disruptor.BusySpinWaitStrategy;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.TimeUnit;

/**
 * @author Anatoly Kadyshev
 */
public class RingBufferDispatcherVsProcessorBenchmarks {

    @Measurement(iterations = 5, time = 1)
    @Warmup(iterations = 3, time = 1)
    @Fork(value = 1, jvmArgs = { "-Xmx1024m" })
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @State(Scope.Thread)
    public static abstract class AbstractBenchmark {

        @Param ( { "1024", "131072", "1048576" } )
        public int BUFFER_SIZE;

        Event<?> event;

        protected Consumer<Event<?>> consumer;

        @Setup
        public void setup() {
            event = Event.wrap("Hello World!");
            doSetup();
        }

        protected abstract void doSetup();

        @TearDown
        public void tearDown() {
            doTearDown();
        }

        protected abstract void doTearDown();

        @Benchmark
        public void justDispatch() {
            doBenchmark();
        }

        protected abstract void doBenchmark();

        protected void createCustomer() {
            this.consumer = new Consumer<Event<?>>() {
                @Override
                public void accept(Event<?> event) {
                }
            };
        }

    }

    public static class RingBufferDispatcher_Benchmark extends AbstractBenchmark {

        RingBufferDispatcher dispatcher;

        @Override
        protected void doSetup() {
            dispatcher = new RingBufferDispatcher(
                    "dispatcher",
                    BUFFER_SIZE,
                    null,
                    ProducerType.MULTI,
                    new BusySpinWaitStrategy()
            );
            createCustomer();
        }

        @Override
        protected void doTearDown() {
            dispatcher.awaitAndShutdown(5, TimeUnit.SECONDS);
        }

        @Override
        protected void doBenchmark() {
            dispatcher.dispatch(event, consumer, null);
        }

    }

    public static class RingBufferDispatcher3_Benchmark extends AbstractBenchmark {

        private RingBufferDispatcher3 dispatcher;

        @Override
        protected void doSetup() {
            dispatcher = new RingBufferDispatcher3("dispatcher", BUFFER_SIZE, null, ProducerType.MULTI, new BusySpinWaitStrategy());
            createCustomer();
        }

        @Override
        protected void doTearDown() {
            dispatcher.shutdown();
        }

        @Override
        protected void doBenchmark() {
            dispatcher.dispatch(event, consumer, null);
        }
    }

    public static class RingBufferProcessor_Benchmark extends AbstractBenchmark {

        private RingBufferProcessor<Event<?>> processor;

        @Override
        protected void doSetup() {
            processor = RingBufferProcessor.share("processor", BUFFER_SIZE, new BusySpinWaitStrategy());
            processor.subscribe(new Subscriber<Event>() {

                @Override
                public void onSubscribe(Subscription s) {
                    s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Event event) {
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onComplete() {
                }

            });
        }

        @Override
        protected void doTearDown() {
            processor.shutdown();
        }

        @Override
        protected void doBenchmark() {
            processor.onNext(event);
        }
    }


}

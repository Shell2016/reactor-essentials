package ru.michaelshell.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

@Slf4j
class OperatorsTest {

    @Test
    void subscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
                .map(i -> {
                    log.info("Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4,5)
                .verifyComplete();
    }

    @Test
    void publishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
                .map(i -> {
                    log.info("Map1 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map2 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4,5)
                .verifyComplete();
    }

    @Test
    void multipleSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("Map1 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map 2 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4,5)
                .verifyComplete();
    }

    @Test
    void multiplePublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
                .publishOn(Schedulers.single())
                .map(i -> {
                    log.info("Map1 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map2 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4,5)
                .verifyComplete();
    }

    @Test
    void publishAndSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
                .publishOn(Schedulers.single())
                .map(i -> {
                    log.info("Map1 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map2 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4,5)
                .verifyComplete();
    }

    @Test
    void subscribeAndPublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("Map1 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map2 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4,5)
                .verifyComplete();
    }
}

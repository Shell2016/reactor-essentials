package ru.michaelshell.reactive.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
class OperatorsTest {

    @Test
    void subscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
                .map(i -> {
                    log.info("Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic()) //affect entire chain, не зависит от места применения в цепочке
                .map(i -> {
                    log.info("Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void publishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
                .map(i -> {
                    log.info("Map1 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic()) //зависит от места в цепочке, аффектит только идущее следом
                .map(i -> {
                    log.info("Map2 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void multipleSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
                .subscribeOn(Schedulers.single()) //только первый используется
                .map(i -> {
                    log.info("Map1 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic()) // этот игнорируется
                .map(i -> {
                    log.info("Map 2 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void multiplePublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
                .publishOn(Schedulers.single()) //both used
                .map(i -> {
                    log.info("Map1 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic()) //both used
                .map(i -> {
                    log.info("Map2 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4, 5)
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
                .subscribeOn(Schedulers.boundedElastic()) //it's ignored
                .map(i -> {
                    log.info("Map2 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void subscribeAndPublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
                .subscribeOn(Schedulers.single()) //both used
                .map(i -> {
                    log.info("Map1 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic()) //both used
                .map(i -> {
                    log.info("Map2 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void subscribeOnIO() {
        Mono<List<String>> list = Mono.fromCallable(() -> Files.readAllLines(Path.of("text-file")))
                .subscribeOn(Schedulers.boundedElastic());

        list.subscribe(l -> log.info("{}", l));

        StepVerifier.create(list)
                .expectSubscription()
                .thenConsumeWhile(strings -> {
                    Assertions.assertFalse(strings.isEmpty());
                    log.info("size {}", strings.size());
                    return true;
                })
                .verifyComplete();
    }

    @Test
    void switchIfEmpty() {
        Flux<Object> flux = Flux.empty()
                .switchIfEmpty(Flux.just("not empty"))
                .log();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("not empty")
                .verifyComplete();
    }

    @Test
    void defer() throws InterruptedException {
        Mono<Long> just = Mono.just(System.currentTimeMillis());
        Mono<Long> defer = Mono.defer(() -> Mono.just(System.currentTimeMillis()));

        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100L);
        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100L);
        defer.subscribe(l -> log.info("time {}", l));

        AtomicLong atomicLong = new AtomicLong();
        defer.subscribe(atomicLong::set);
//        System.out.println(atomicLong.get());
        Assertions.assertTrue(atomicLong.get() > 0);
    }

    @Test
    void concatOperator() {
        Flux<Integer> flux1 = Flux.just(1, 2).delayElements(Duration.ofMillis(200));
        Flux<Integer> flux2 = Flux.just(3, 4);

//        Flux<Integer> concat = Flux.concat(flux1, flux2);
        Flux<Integer> concat = flux1.concatWith(flux2).log();


        StepVerifier.create(concat)
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    void combineLatest() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> combinedLatest = Flux.combineLatest(flux1, flux2, (s1, s2) -> s1.toUpperCase() + s2.toUpperCase());

        StepVerifier.create(combinedLatest)
                .expectNext("BC", "BD")
                .verifyComplete();
    }

    @Test
    void merge() {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> merged = Flux.merge(flux1, flux2);  //eager

        merged.subscribe(log::info);

        StepVerifier.create(merged)
                .expectNext("c", "d", "a", "b")
                .verifyComplete();
    }

    @Test
    void mergeSequential() {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> merged = Flux.mergeSequential(flux1, flux2, flux1);

        merged.subscribe(log::info);

        StepVerifier.create(merged)
                .expectSubscription()
                .expectNext("a", "b", "c", "d", "a", "b")
                .verifyComplete();
    }

    @Test
    void concatDelayError() {
        Flux<String> flux1 = Flux.just("a", "b")
                .handle((s, sink) -> {
                    if (Objects.equals(s, "b")) {
                        sink.error(new RuntimeException());
                        return;
                    }
                    sink.next(s);

                });
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> merged = Flux.concatDelayError(flux1, flux2).log();

        StepVerifier.create(merged)
                .expectNext("a", "c", "d")
                .expectError()
                .verify();
    }

    @Test
    void mergeDelayError() {
        Flux<String> flux1 = Flux.just("a", "b")
                .<String>handle((s, sink) -> {
                    if ("b".equals(s)) {
                        sink.error(new RuntimeException());
                        return;
                    }
                    sink.next(s);
                })
                .doOnError(throwable -> log.info("Do something here"));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> merged = Flux.mergeDelayError(1, flux1, flux2, flux1).log();

        merged.subscribe(log::info);
        System.out.println("----------------------------------------");

        StepVerifier.create(merged)
                .expectNext("a", "c", "d", "a")
                .expectError()
                .verify();
    }

    @Test
    void flatMap() {
        Flux<String> flux = Flux.just("a", "b");
//        Flux<Flux<String>> fluxFlux = flux.map(String::toUpperCase)
//                .map(this::findByName)
//                .log();

        Flux<String> flatFlux = flux.map(String::toUpperCase)
                .flatMap(this::findByName)
                .log();

        flatFlux.subscribe();

        StepVerifier
                .create(flatFlux)
                .expectSubscription()
                .expectNext("nameA1", "nameA2", "nameB1", "nameB2")
                .verifyComplete();

    }

    @Test
    void flatMapSequential() {
        Flux<String> flux = Flux.just("a", "b");

        Flux<String> flatFlux = flux.map(String::toUpperCase)
                .flatMapSequential(this::findByName)
                .log();

        flatFlux.subscribe();

        StepVerifier
                .create(flatFlux)
                .expectSubscription()
                .expectNext("nameA1", "nameA2", "nameB1", "nameB2")
                .verifyComplete();
    }

    private Flux<String> findByName(String name) {
        return name.equals("A")
                ? Flux.just("nameA1", "nameA2").delayElements(Duration.ofMillis(200))
                : Flux.just("nameB1", "nameB2");
    }

    @Test
    void zip() {
        Flux<String> titlesFlux = Flux.just("Grand Blue", "Baki");
        Flux<String> studiosFlux = Flux.just("Zero-G", "TMS Entertainment");
        Flux<Integer> episodesFlux = Flux.just(12, 24);


        Flux<Anime> animeFlux = Flux.zip(titlesFlux, studiosFlux, episodesFlux)
                .flatMap(objects -> Flux.just(new Anime(objects.getT1(), objects.getT2(), objects.getT3()))).log();

//        animeFlux.subscribe(anime -> log.info(anime.toString()));

        StepVerifier
                .create(animeFlux)
                .expectSubscription()
                .expectNext(new Anime("Grand Blue", "Zero-G", 12),
                        new Anime("Baki", "TMS Entertainment", 24))
                .verifyComplete();
    }

    @Data
    @AllArgsConstructor
    class Anime {
        private String title;
        private String studio;
        private int episodes;
    }

}

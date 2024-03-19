package org.example.testassignmentapicall;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.experimental.FieldDefaults;
import lombok.experimental.StandardException;
import org.springframework.http.HttpStatusCode;
import org.springframework.lang.Nullable;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static lombok.AccessLevel.PRIVATE;
import static org.example.testassignmentapicall.CrptApi.ApiPayload.*;
import static org.example.testassignmentapicall.CrptApi.SubmitProductReturn.Outcome.RATE_LIMIT_HIT;
import static org.example.testassignmentapicall.CrptApi.SubmitProductReturn.Outcome.SUCCESS;
import static org.springframework.http.MediaType.APPLICATION_JSON;

@RequiredArgsConstructor
@FieldDefaults(level = PRIVATE, makeFinal = true)
public class CrptApi {
    WebClient webClient;
    RateLimiter rateLimiter;

    public static CrptApi exampleFactory(TimeUnit timeUnit, int requestTimeout) {
        return new CrptApi(WebClient.create().mutate().baseUrl("https://ismp.crpt.ru/").build(),
                new RateLimiter(timeUnit, requestTimeout, Clock.systemUTC()));
    }

    @SneakyThrows
    public static void main(String[] args) {
        CrptApi crptReactiveApi = new CrptApi(
                WebClient.builder().baseUrl("http://localhost:8080").build(),
                new CrptApi.RateLimiter(TimeUnit.SECONDS, 1, Clock.systemDefaultZone()));

        SubmitProductReturn block1 = crptReactiveApi.submitProduct(builder().build())
                .block();

        SubmitProductReturn block2 = crptReactiveApi.submitProduct(builder().build())
                .block();

        Thread.sleep(1005);

        SubmitProductReturn block3 = crptReactiveApi.submitProduct(builder().build())
                .block();
        System.out.println(block1);
        System.out.println(block2);
        System.out.println(block3);

        try {
            SubmitProductReturn block4 = CrptApi.exampleFactory(TimeUnit.DAYS, 1).submitProduct(builder().build())
                    .block();
        } catch (CrptException e) {
            System.out.println(e.getMessage());
        }

        // Если запустить программу будет такой вывод:
        //   CrptApi.SubmitProductReturn(outcome=SUCCESS)
        //   CrptApi.SubmitProductReturn(outcome=RATE_LIMIT_HIT)
        //   CrptApi.SubmitProductReturn(outcome=SUCCESS)
        //   Client error: Full authentication is required to access this resource
    }

    public Mono<SubmitProductReturn> submitProduct(@NonNull ApiPayload payload) {
        if (!rateLimiter.spentRequestPoint()) {
            return Mono.just(new SubmitProductReturn(RATE_LIMIT_HIT, null));
        }
        return webClient.post()
                .uri("api/v3/lk/documents/create")
                .contentType(APPLICATION_JSON)
                .body(BodyInserters.fromValue(payload))
                .retrieve()
                .onStatus(statusCode -> statusCode.is4xxClientError(),
                        clientResponse -> clientResponse.bodyToMono(SubmitErrorResponseReturnPayload.class)
                                .map(SubmitErrorResponseReturnPayload::getMessage)
                                .map(errorMessage -> new CrptException(String.format("Client error: %s", errorMessage)))
                                .flatMap(Mono::error))
                .onStatus(HttpStatusCode::is5xxServerError,
                        clientResponse -> Mono.error(new CrptException("Server error")))
                .bodyToMono(SubmitResponseReturnPayload.class)
                .map(response -> new SubmitProductReturn(SUCCESS, response));
    }

    @Data
    static class SubmitResponseReturnPayload {
        // Какие-то поля, которые возвращаются в успешном ответе
        String id;
    }

    @Data
    static class SubmitErrorResponseReturnPayload {
        // Какие-то поля, которые возвращаются в ошибке со стороны сервера, похоже на спринговые DefaultErrorAttributes
        String message;
        String trace;
        String requestId;
        Integer status;
        String path;
    }

    @RequiredArgsConstructor
    @FieldDefaults(level = PRIVATE, makeFinal = true)
    static class RateLimiter {
        TimeUnit timeUnit;
        int requestLimit;
        LinkedList<Instant> queue = new LinkedList<>();
        Clock now;

        synchronized boolean spentRequestPoint() {
            queue.removeIf(instant -> instant.isBefore(now.instant().minus(1, timeUnit.toChronoUnit())));
            if (queue.size() < requestLimit) {
                queue.push(now.instant());
                return true;
            }
            return false;
        }
    }

    @Value
    @RequiredArgsConstructor(access = PRIVATE)
    public static class SubmitProductReturn {
        Outcome outcome;
        @Nullable
        SubmitResponseReturnPayload response;

        public enum Outcome {
            SUCCESS, RATE_LIMIT_HIT
        }

    }

    @Value
    @Builder
    public static class ApiPayload {
        Description description;
        String doc_id;
        String doc_status;
        String doc_type;
        boolean importRequest;
        String owner_inn;
        String participant_inn;
        String producer_inn;
        String production_date;
        String production_type;
        List<Product> products;
        String reg_date;
        String reg_number;

        @Value
        @Builder
        public static class Description {
            String participantInn;
        }

        @Value
        @Builder
        public static class Product {
            String certificate_document;
            String certificate_document_date;
            String certificate_document_number;
            String owner_inn;
            String producer_inn;
            String production_date;
            String tnved_code;
            String uit_code;
            String uitu_code;
        }
    }

    @StandardException
    static class CrptException extends RuntimeException {
    }
}

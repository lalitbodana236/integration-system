
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class IntegrationLoadTest {

    private static final String BASE_URL =
            "http://localhost:10001/api";

    // Load configuration
    private static final int THREAD_POOL_SIZE = 200;

    // Start small first
    private static final int REQUESTS_PER_API = 1000;

    // APIs
    private static final List<String> APIS = List.of(
            "po",
            "so",
            "inventory",
            "checklist",
            "location"
    );

    private static final HttpClient CLIENT =
            HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .version(HttpClient.Version.HTTP_1_1)
                    .build();

    // Metrics
    private static final AtomicInteger SUCCESS =
            new AtomicInteger();

    private static final AtomicInteger FAILURE =
            new AtomicInteger();

    private static final AtomicInteger DUPLICATE =
            new AtomicInteger();

    private static final AtomicInteger TIMEOUT =
            new AtomicInteger();

    private static final AtomicLong TOTAL_LATENCY =
            new AtomicLong();

    public static void main(String[] args) throws Exception {

        ExecutorService executor =
                Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        int totalRequests =
                REQUESTS_PER_API * APIS.size();

        CountDownLatch latch =
                new CountDownLatch(totalRequests);

        long globalStart =
                System.currentTimeMillis();

        for (String api : APIS) {

            for (int i = 1; i <= REQUESTS_PER_API; i++) {

                final int id = i;

                executor.submit(() -> {

                    try {

                        sendRequest(api, id);

                    } catch (HttpTimeoutException e) {

                        TIMEOUT.incrementAndGet();

                        System.err.println(
                                "[TIMEOUT] api=" + api +
                                " id=" + id
                        );

                    } catch (Exception e) {

                        FAILURE.incrementAndGet();

                        System.err.println(
                                "[ERROR] api=" + api +
                                " id=" + id +
                                " error=" + e.getMessage()
                        );

                    } finally {
                        latch.countDown();
                    }
                });
            }
        }

        latch.await();

        long globalEnd =
                System.currentTimeMillis();

        printSummary(
                globalStart,
                globalEnd,
                totalRequests
        );

        executor.shutdown();
    }

    private static void sendRequest(
            String api,
            int id
    ) throws Exception {

        String region = resolveRegion(id);

        String correlationId =
                UUID.randomUUID().toString();

        // Simulate duplicate traffic
        String eventId;

        if (id % 10 == 0) {

            eventId =
                    "DUPLICATE-" + (id / 10);

        } else {

            eventId =
                    UUID.randomUUID().toString();
        }

        String customerId =
                "CUSTOMER-" + (id % 1000);

        String requestId =
                api + "-" + id;

        String url =
                BASE_URL +
                        "/" + api +
                        "?id=" + requestId +
                        "&customerId=" + customerId +
                        "&region=" + region;

        String payload =
                buildPayload(api, id, region);

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(10))
                        .header(
                                "Content-Type",
                                "application/json"
                        )
                        .header(
                                "X-Event-Id",
                                eventId
                        )
                        .header(
                                "X-Correlation-Id",
                                correlationId
                        )
                        .POST(
                                HttpRequest.BodyPublishers
                                        .ofString(payload)
                        )
                        .build();

        long start =
                System.currentTimeMillis();

        HttpResponse<String> response =
                CLIENT.send(
                        request,
                        HttpResponse.BodyHandlers.ofString()
                );

        long end =
                System.currentTimeMillis();

        long latency =
                end - start;

        TOTAL_LATENCY.addAndGet(latency);

        String responseBody =
                response.body();

        if (responseBody.contains("DUPLICATE")) {

            DUPLICATE.incrementAndGet();

        } else if (
                response.statusCode() == 200 ||
                response.statusCode() == 202
        ) {

            SUCCESS.incrementAndGet();

        } else {

            FAILURE.incrementAndGet();

            System.err.println(
                    "[FAILED] api=" + api +
                    " status=" + response.statusCode() +
                    " response=" + responseBody
            );
        }

        // Retry simulation
        if (id % 20 == 0) {

            CLIENT.send(
                    request,
                    HttpResponse.BodyHandlers.ofString()
            );
        }

        // Success sampling logs
        if (id % 500 == 0) {

            System.out.println(
                    "[PROGRESS] api=" + api +
                    " processed=" + id
            );
        }
    }

    private static String resolveRegion(int id) {

        int traffic = id % 100;

        // realistic regional traffic split

        if (traffic < 70) {
            return "INDIA";
        }

        if (traffic < 90) {
            return "US";
        }

        return "UK";
    }

    private static String buildPayload(
            String api,
            int id,
            String region
    ) {

        return """
                {
                  "eventType": "%s",
                  "orderId": "%s-%05d",
                  "customerId": "CUST-%d",
                  "region": "%s",
                  "item": "Laptop",
                  "quantity": %d,
                  "price": %d,
                  "warehouse": "WH-%d",
                  "timestamp": %d
                }
                """.formatted(
                api.toUpperCase(),
                api.toUpperCase(),
                id,
                id % 1000,
                region,
                (id % 5) + 1,
                50000 + id,
                id % 20,
                System.currentTimeMillis()
        );
    }

    private static void printSummary(
            long start,
            long end,
            int totalRequests
    ) {

        long totalTime =
                end - start;

        long avgLatency =
                SUCCESS.get() == 0
                        ? 0
                        : TOTAL_LATENCY.get() / SUCCESS.get();

        double throughput =
                (totalRequests * 1000.0) / totalTime;

        System.out.println();
        System.out.println(
                "======================================"
        );

        System.out.println(
                " INTEGRATION LOAD TEST SUMMARY"
        );

        System.out.println(
                "======================================"
        );

        System.out.println(
                "Total APIs          : " + APIS.size()
        );

        System.out.println(
                "Requests/API        : " + REQUESTS_PER_API
        );

        System.out.println(
                "Total Requests      : " + totalRequests
        );

        System.out.println(
                "--------------------------------------"
        );

        System.out.println(
                "Success Count       : " + SUCCESS.get()
        );

        System.out.println(
                "Duplicate Count     : " + DUPLICATE.get()
        );

        System.out.println(
                "Failure Count       : " + FAILURE.get()
        );

        System.out.println(
                "Timeout Count       : " + TIMEOUT.get()
        );

        System.out.println(
                "--------------------------------------"
        );

        System.out.println(
                "Total Time(ms)      : " + totalTime
        );

        System.out.println(
                "Average Latency(ms) : " + avgLatency
        );

        System.out.println(
                "Throughput(req/sec) : " +
                        String.format("%.2f", throughput)
        );

        System.out.println(
                "======================================"
        );
    }
}


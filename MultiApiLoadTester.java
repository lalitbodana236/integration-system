import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.*;

public class MultiApiLoadTester {

    private static final String BASE_URL = "http://localhost:10001/api";

    private static final int REQUESTS_PER_API = 10000;
    private static final int THREAD_POOL_SIZE = 100;

    private static final HttpClient client = HttpClient.newHttpClient();

    // APIs to test
    private static final List<String> APIS = List.of(
            "po",
            "so",
            "inventory",
            "checklist",
            "location"
    );

    public static void main(String[] args) throws Exception {

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        int totalRequests = REQUESTS_PER_API * APIS.size();
        CountDownLatch latch = new CountDownLatch(totalRequests);

        long start = System.currentTimeMillis();

        for (String api : APIS) {
            for (int i = 1; i <= REQUESTS_PER_API; i++) {
                final int id = i;
                executor.submit(() -> {
                    try {
                        sendRequest(api, id);
                    } catch (Exception e) {
                        System.err.println("Error API=" + api + " id=" + id + " -> " + e.getMessage());
                    } finally {
                        latch.countDown();
                    }
                });
            }
        }

        latch.await();

        long end = System.currentTimeMillis();

        System.out.println("=====================================");
        System.out.println("Total APIs       : " + APIS.size());
        System.out.println("Requests per API : " + REQUESTS_PER_API);
        System.out.println("Total Requests   : " + totalRequests);
        System.out.println("Time Taken (ms)  : " + (end - start));
        System.out.println("=====================================");

        executor.shutdown();
    }

    private static void sendRequest(String api, int id) throws Exception {

        String url = BASE_URL + "/" + api + "?id=" + api + "-" + id;

        String payload = """
                {
                  "orderId": "%s-%03d",
                  "item": "Laptop",
                  "quantity": 2
                }
                """.formatted(api.toUpperCase(), id);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

        HttpResponse<String> response =
                client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            System.err.println("Failed API=" + api + " id=" + id + " status=" + response.statusCode());
        }
    }
}
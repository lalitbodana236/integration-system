import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.*;

public class LoadTester {

    private static final String BASE_URL = "http://localhost:10001/api";
    private static final int TOTAL_REQUESTS = 1000;
    private static final int THREAD_POOL_SIZE = 50; // tune this

    private static final HttpClient client = HttpClient.newHttpClient();

    public static void main(String[] args) throws Exception {

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        long start = System.currentTimeMillis();

        CountDownLatch latch = new CountDownLatch(TOTAL_REQUESTS);

        for (int i = 1; i <= TOTAL_REQUESTS; i++) {
            final int id = i;

            executor.submit(() -> {
                try {
                    sendRequest("po", id);
                    // You can test others:
                    // sendRequest("so", id);
                    // sendRequest("inventory", id);

                } catch (Exception e) {
                    System.err.println("Error for id=" + id + " : " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(); // wait for all requests

        long end = System.currentTimeMillis();

        System.out.println("Completed " + TOTAL_REQUESTS + " requests in " + (end - start) + " ms");

        executor.shutdown();
    }

    private static void sendRequest(String api, int id) throws Exception {

        String url = BASE_URL + "/" + api + "?id=" + id;

        String payload = """
                {
                  "orderId": "PO-%03d",
                  "item": "Laptop",
                  "quantity": 2
                }
                """.formatted(id);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

        HttpResponse<String> response =
                client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            System.err.println("Failed: " + id + " status=" + response.statusCode());
        }
    }
}
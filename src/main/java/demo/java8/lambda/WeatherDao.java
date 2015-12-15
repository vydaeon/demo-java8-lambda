package demo.java8.lambda;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class WeatherDao {

    private static final WeatherDao INSTANCE = new WeatherDao();

    public static WeatherDao getInstance() {
        return INSTANCE;
    }

    private WeatherDao() {
    }

    public CompletionStage<Map<Integer, IntSummaryStatistics>> getWeather(String airportCode, int month,
                                                                          int startYear, int endYear) {
        CloseableHttpAsyncClient client = HttpAsyncClients.createDefault();
        client.start();

        List<CompletableFuture<Map.Entry<Integer, String>>> csvForYearFutures =
                IntStream.rangeClosed(startYear, endYear)
                        .mapToObj(year -> getCsvForYear(client, airportCode, month, year))
                        .collect(toList());

        //close the client after all CSV requests are done
        CompletableFuture<?>[] futures = csvForYearFutures.stream().toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).thenRun(() -> close(client));

        return csvForYearFutures.stream()
                .map(this::toSummaryStatistics)
                .reduce(CompletableFuture.completedFuture(new TreeMap<>()),
                        this::addEntry,
                        this::combine);
    }

    private CompletableFuture<Map.Entry<Integer, String>> getCsvForYear(CloseableHttpAsyncClient client,
                                                                        String airportCode, int month, int year) {
        String uri = String.format(
                "http://www.wunderground.com/history/airport/%s/%s/%s/1/MonthlyHistory.html?format=1",
                airportCode, year, month);
        HttpGet httpGet = new HttpGet(uri);
        CompletableFuture<Map.Entry<Integer, String>> promise = new CompletableFuture<>();
        client.execute(httpGet, new FutureCallback<HttpResponse>() {
            @Override
            public void completed(HttpResponse result) {
                try {
                    String csv = EntityUtils.toString(result.getEntity());
                    Map.Entry<Integer, String> csvForYear = new SimpleImmutableEntry<>(year, csv);
                    promise.complete(csvForYear);
                } catch (Exception e) {
                    promise.completeExceptionally(e);
                }
            }

            @Override
            public void failed(Exception e) {
                promise.completeExceptionally(e);
            }

            @Override
            public void cancelled() {
                promise.cancel(false);
            }
        });
        return promise;
    }

    private void close(CloseableHttpAsyncClient client) {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private CompletableFuture<Map.Entry<Integer, IntSummaryStatistics>> toSummaryStatistics(
            CompletableFuture<Map.Entry<Integer, String>> csvByYearFuture) {
        return csvByYearFuture.thenApplyAsync(csvByYear -> {
            String csv = csvByYear.getValue();
            IntSummaryStatistics summaryStatistics = Stream.of(csv.trim().split("\\n"))
                    .skip(1) //skip header row
                    .map(this::toMeanTempString)
                    .filter(meanTempString -> !meanTempString.isEmpty())
                    .mapToInt(Integer::parseInt)
                    .summaryStatistics();
            return new SimpleImmutableEntry<>(csvByYear.getKey(), summaryStatistics);
        });
    }

    private String toMeanTempString(String csvLine) {
        Optional<String> meanTempStringOption = Stream.of(csvLine.split(","))
                .skip(2) //skip date and maxTemp columns
                .findFirst();
        return meanTempStringOption.orElse("");
    }

    private CompletableFuture<Map<Integer, IntSummaryStatistics>> addEntry(
            CompletableFuture<Map<Integer, IntSummaryStatistics>> mapFuture,
            CompletableFuture<Map.Entry<Integer, IntSummaryStatistics>> entryFuture) {
        return mapFuture.thenCombineAsync(entryFuture, (map, entry) -> {
            map.put(entry.getKey(), entry.getValue());
            return map;
        });
    }

    private CompletableFuture<Map<Integer, IntSummaryStatistics>> combine(
            CompletableFuture<Map<Integer, IntSummaryStatistics>> mapFuture1,
            CompletableFuture<Map<Integer, IntSummaryStatistics>> mapFuture2) {
        return mapFuture1.thenCombine(mapFuture2, (map1, map2) -> {
            map1.putAll(map2);
            return map1;
        });
    }
}

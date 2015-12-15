package demo.java8.lambda;

import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import java.util.IntSummaryStatistics;
import java.util.Map;
import java.util.stream.Collectors;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("weather")
public class WebService {

    private static final int DEFAULT_START_YEAR = 1950;
    private static final int DEFAULT_END_YEAR = 2014;

    @GET
    @Path("{airportCode}/{month}")
    @Produces(APPLICATION_JSON)
    public void startWeatherQuery(@PathParam("airportCode") String airportCode,
                                  @PathParam("month") int month,
                                  @QueryParam("startYear") int startYear,
                                  @QueryParam("endYear") int endYear,
                                  @Suspended AsyncResponse asyncResponse) {
        if (startYear <= 0) {
            startYear = DEFAULT_START_YEAR;
        }
        if (endYear <= 0) {
            endYear = DEFAULT_END_YEAR;
        }
        if (endYear < startYear) {
            int temp = startYear;
            startYear = endYear;
            endYear = temp;
        }

        WeatherDao.getInstance()
                .getWeather(airportCode, month, startYear, endYear)
                .thenApply(this::toJson)
                .whenCompleteAsync((weatherJson, throwable) -> {
                    if (throwable != null) {
                        asyncResponse.resume(throwable);
                    } else {
                        asyncResponse.resume(weatherJson);
                    }
                });
    }

    private String toJson(Map<Integer, IntSummaryStatistics> statisticsByYear) {
        return statisticsByYear.entrySet().stream()
                .map(this::toJson)
                .collect(Collectors.joining(",", "{", "}"));
    }

    private String toJson(Map.Entry<Integer, IntSummaryStatistics> statisticByYear) {
        Integer year = statisticByYear.getKey();
        IntSummaryStatistics statistics = statisticByYear.getValue();
        return "\"" + year + "\"" + ":" + toJson(statistics);
    }

    private String toJson(IntSummaryStatistics statistics) {
        return String.format("{\"days\":%s,\"minDailyMeanTemp\":%s,\"meanDailyMeanTemp\":%s,\"maxDailyMeanTemp\":%s}",
                statistics.getCount(),
                statistics.getMin(),
                statistics.getAverage(),
                statistics.getMax());
    }
}

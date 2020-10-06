package com.kolawole1.futures.app;

import com.kolawole1.threads.service.ThreadBasedPlanetFileAnalyzer;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class FutureBasedPlanetAnalyzerApp {


    private static ThreadBasedPlanetFileAnalyzer fileAnalyzer = new ThreadBasedPlanetFileAnalyzer();

    public static void main(String[] files) throws InterruptedException, ExecutionException, URISyntaxException {
        Double meanTemperature = getAverageOfTemperatureFiles(files);
        System.out.println("Calculated a mean of " + meanTemperature);
    }

    private static Double getAverageOfTemperatureFiles(String[] files) throws ExecutionException, InterruptedException, URISyntaxException {

        String fileOne = files[0];
        String fileTwo = files[1];
        Path fileOnePath = getCSVPath(fileOne);
        Path fileTwoPath = getCSVPath(fileTwo);


        ExecutorService executorService = Executors.newFixedThreadPool(4);


        Future<Double> futureOfSumOne = executorService.submit(()-> fileAnalyzer.sumFile(fileOnePath));
        Future<Double> futureOfSumTwo = executorService.submit(()-> fileAnalyzer.sumFile(fileTwoPath));


        Future<Double> futureOfCountOne = executorService.submit(()-> fileAnalyzer.countDoubleRows(fileOnePath));
        Future<Double> futureOfCountTwo = executorService.submit(()-> fileAnalyzer.countDoubleRows(fileTwoPath));


        Double valueOfFileOneSum = futureOfSumOne.get();
        Double valueOfFileTwoSum = futureOfSumTwo.get();

        Double valueOfFileOneCount = futureOfCountOne.get();
        Double valueOfFileTwoCount = futureOfCountTwo.get();


        Future<Double> futureAverage = executorService.submit(
                () -> {

                    Double sumOfTemperatures = valueOfFileOneSum + valueOfFileTwoSum;


                    Double countOfTemperatures = valueOfFileOneCount + valueOfFileTwoCount;


                    return sumOfTemperatures / countOfTemperatures;
                });


        executorService.shutdown();


        return futureAverage.get();
    }

    private static Path getCSVPath(String file) throws URISyntaxException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL resource = loader.getResource(file);
        if (resource == null) {
            throw new RuntimeException("Bad file name provided");
        }
        return Paths.get(resource.toURI());
    }

}

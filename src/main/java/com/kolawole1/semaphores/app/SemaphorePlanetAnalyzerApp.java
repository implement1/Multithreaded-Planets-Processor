package com.kolawole1.semaphores.app;

import com.kolawole1.threads.service.ThreadBasedPlanetFileAnalyzer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class SemaphorePlanetAnalyzerApp {
    private static ThreadBasedPlanetFileAnalyzer fileAnalyzer = new ThreadBasedPlanetFileAnalyzer();

    // Create Semaphore of Size 8
    static Semaphore semaphore = new Semaphore(8);

    public static void main(String[] folders) throws IOException, ExecutionException, InterruptedException, URISyntaxException {
        Double average = getAveragesFromFolder(folders);

        System.out.println("Calculated an average of "+ average);
    }

    public static Double getAveragesFromFolder(String[] folders) throws IOException, ExecutionException, InterruptedException, URISyntaxException {


        ExecutorService executorService = Executors.newFixedThreadPool(20);

        try {

            List<Future<Double>> futuresListOfFileSums = new ArrayList<>();
            List<Future<Double>> futuresListOfSampleCounts = new ArrayList<>();


            for (String folder : folders) {
                ClassLoader loader = Thread.currentThread().getContextClassLoader();
                URL folderResource = loader.getResource(folders[0]);

                Files.walk(Paths.get(folderResource.toURI()))
                        .filter(Files::isRegularFile)
                        .forEach(filePath -> {


                            Future<Double> summingTask = executorService.submit(() -> sumFileWithSemaphores(filePath));
                            Future<Double> countingTask = executorService.submit(() -> countFileWithSemaphores(filePath));


                            futuresListOfFileSums.add(summingTask);
                            futuresListOfSampleCounts.add(countingTask);
                        });
            }


            Double sumOfTemperatures = 0.0;
            for (Future<Double> future : futuresListOfFileSums) {
                sumOfTemperatures += future.get();
            }


            Double totalSampleSize = 0.0;
            for (Future<Double> future : futuresListOfSampleCounts) {
                totalSampleSize += future.get();
            }


            return sumOfTemperatures / totalSampleSize;
        } finally {
            executorService.shutdown();
        }
    }

    private static Double sumFileWithSemaphores(Path path) throws InterruptedException {
        Double result = null;
        try {

            semaphore.acquire();
            result = fileAnalyzer.sumFile(path);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            semaphore.release();
        }
        return result;
    }

    private static Double countFileWithSemaphores(Path path) throws InterruptedException {
        Double result = null;
        try {

            semaphore.acquire();
            result = fileAnalyzer.countDoubleRows(path);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            semaphore.release();
        }
        return result;
    }

}

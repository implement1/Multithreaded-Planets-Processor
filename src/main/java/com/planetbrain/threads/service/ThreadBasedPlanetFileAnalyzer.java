package com.planetbrain.threads.service;

import com.planetbrain.model.KeplerCsvFields;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.DoubleStream;

public class ThreadBasedPlanetFileAnalyzer {


    /**
     * Returns a DoubleStream of temperature values

     */
    private DoubleStream getDoublesFromFile(Path path) throws IOException {
        DoubleStream streamOfDoubles = Files.lines(path).

                        filter(line -> !line.startsWith(KeplerCsvFields.COMMENT_CHARACTER)).

                        filter(line -> !line.contains("kepoi_name,kepler_name,koi_teq")).

                        map(line -> line.split(",")).
                        filter(row -> row.length >= 3).

                        map(row -> row[KeplerCsvFields.EQUILIBRIUM_TEMPERATURE_COLUMN]).

                        mapToDouble(temperatureStringValue -> Double.parseDouble(temperatureStringValue));
        return streamOfDoubles;
    }

    /**
     * Returns the sum of all temperatures in a file

     */
    public Double sumFile(Path path) throws IOException {
        DoubleStream temperaturesInFile = getDoublesFromFile(path);
        return temperaturesInFile.sum();
    }

    /**
     * Returns a count of all temperatures in a file

     */
    public Double countDoubleRows(Path path) throws IOException {
        DoubleStream temperaturesInFile = getDoublesFromFile(path);
        return (double) temperaturesInFile.count();
    }
}

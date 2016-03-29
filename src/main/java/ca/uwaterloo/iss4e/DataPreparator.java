package ca.uwaterloo.iss4e;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

public class DataPreparator {

    private static final Logger log = Logger.getLogger(DataPreparator.class);


    public static double getMean(List<Double> data) {
        double sum = 0.0;
        for (double a : data)
            sum += a;
        return sum / data.size();
    }

    public static double getStdDev(List<Double> data) {
        double mean = getMean(data);
        double temp = 0;
        for (double a : data)
            temp += (mean - a) * (mean - a);
        return Math.sqrt(temp / data.size());
    }



    static class Split implements PairFunction<String, Tuple2<Integer, Integer>, Double[]> {
        @Override
        public Tuple2<Tuple2<Integer, Integer>, Double[]> call(String line) throws Exception {
            //MeterID|TimeStamp|Reading|WeatherTemperature
            String[] fields = line.split(",");
            int meterID = Integer.valueOf(fields[0]); //MeterID
            double readate = 1.0 * Integer.valueOf(fields[1].replace("-", ""));
            int hour = Integer.valueOf(fields[2]);
            double reading = Double.parseDouble(fields[3]);
            double temperature = Double.parseDouble(fields[4]);
            return new Tuple2<Tuple2<Integer, Integer>, Double[]>( // (meterID, hour)--> (reading, temperature, readdate)
                    new Tuple2<Integer, Integer>(meterID, hour), new Double[]{reading, temperature, readate});
        }
    }

    public static void train(String inputDir, String outputPath) {
        SparkConf conf = new SparkConf().setAppName("DataPreparator");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<Tuple2<Integer, Integer>, Double[]> parsedData = sc.textFile(inputDir)
                .mapToPair(new Split())
                .cache();

        JavaPairRDD<Tuple2<Integer, Integer>, Double[]> models = parsedData.groupByKey()
                .mapValues(new Function<Iterable<Double[]>, Double[]>() {
                    @Override
                    public Double[] call(Iterable<Double[]> values) throws Exception {
                        try {
                            OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
                            Iterator<Double[]> itr = values.iterator();
                            ArrayList<Double[]> readTempDateList = new ArrayList<Double[]>();
                            while (itr.hasNext()) {
                                readTempDateList.add(itr.next());
                            }
                            Collections.sort(readTempDateList, new Comparator<Double[]>() {
                                @Override
                                public int compare(Double[] readTempDate1, Double[] readTempDate2) {
                                    return readTempDate1[2] < readTempDate2[2] ? -1 : 1;
                                }
                            });
                            int nDay = readTempDateList.size();
                            double[] Y = new double[nDay - 3];
                            double[][] X = new double[nDay - 3][];
                            for (int i = 0; i < nDay - 3; ++i) {
                                double[] x = new double[6];
                                Double[] readingTemp0 = readTempDateList.get(i);
                                Double[] readingTemp1 = readTempDateList.get(i + 1);
                                Double[] readingTemp2 = readTempDateList.get(i + 2);
                                Double[] readingTemp3 = readTempDateList.get(i + 3);
                                Y[i] = readingTemp3[0].doubleValue();
                                x[0] = readingTemp2[0].doubleValue();
                                x[1] = readingTemp1[0].doubleValue();
                                x[2] = readingTemp0[0].doubleValue();

                                double temp = readingTemp3[1].doubleValue();
                                x[3] = temp > 20 ? temp - 20 : 0;
                                x[4] = temp < 16 ? 16 - temp : 0;
                                x[5] = temp < 5 ? 5 - temp : 0;
                                X[i] = x;
                            }
                            regression.newSampleData(Y, X);
                            double[] parameters = regression.estimateRegressionParameters();
                            return ArrayUtils.toObject(parameters); //(meterID, hour)-> parameters
                        } catch (Exception e) {
                            e.printStackTrace();
                            return new Double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
                        }
                    }
                });

        parsedData
                .cogroup(models)
                .flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Iterable<Double[]>, Iterable<Double[]>>>, Integer, Double[]>() {
                    @Override
                    public Iterable<Tuple2<Integer, Double[]>> call(Tuple2<Tuple2<Integer, Integer>, Tuple2<Iterable<Double[]>, Iterable<Double[]>>> tuple) throws Exception {
                        Tuple2<Integer, Integer> meterIDHour = tuple._1;
                        int meterID = meterIDHour._1;
                        double hour = 1.0*meterIDHour._2;
                        Iterator<Double[]> itr1 = tuple._2._1().iterator(); // Double[] = {reading, temperature, readdate}
                        Iterator<Double[]> itr2 = tuple._2._2().iterator(); // Double[] = {bata0, beta1, ..., ,beta6}
                        List<Tuple2<Integer, Double[]>> ret = new ArrayList<Tuple2<Integer, Double[]>>();
                        if (itr2.hasNext()) {
                            Double[] beta = itr2.next();
                            List<Double> activityReadings = new ArrayList<Double>();
                            while (itr1.hasNext()) {
                                Double[] readingTempDate = itr1.next();
                                double[] x = new double[3];
                                double temp = readingTempDate[1].doubleValue();
                                x[0] = temp > 20 ? temp - 20 : 0;
                                x[1] = temp < 16 ? 16 - temp : 0;
                                x[2] = temp < 5 ? 5 - temp : 0;
                                double activityReading = readingTempDate[0].doubleValue();
                                for (int i = 0; i < 3; ++i) {
                                    activityReading -= beta[4 + i] * x[i];
                                }
                                activityReadings.add(activityReading);
                            }
                            double mean = DataPreparator.getMean(activityReadings);
                            double stddev = DataPreparator.getStdDev(activityReadings);
                            ret.add(new Tuple2<Integer, Double[]>(meterID, new Double[]{hour, mean, stddev}));
                        }
                        return ret;
                    }
                })
                .groupByKey()
                .mapToPair(new PairFunction<Tuple2<Integer, Iterable<Double[]>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Integer, Iterable<Double[]>> tuple) throws Exception {
                        int meterID = tuple._1;
                        Iterator<Double[]> itr = tuple._2.iterator();
                        double[] hourlyReadings = new double[24];
                        double[] stdDevs = new double[24];
                        while (itr.hasNext()) {
                            Double[] hourMeanStddev = itr.next();
                            int hour = (int) hourMeanStddev[0].doubleValue();
                            hourlyReadings[hour] = hourMeanStddev[1].doubleValue();
                            stdDevs[hour] = hourMeanStddev[2].doubleValue();
                        }
                        StringBuffer row = new StringBuffer();
                        row.append(meterID).append("|");
                        for (int i = 0; i < 24; ++i) {
                            row.append(hourlyReadings[i]);
                            if (i<23) row.append(",");
                        }
                        row.append("|");
                        for (int i = 0; i < 24; ++i) {
                            row.append(stdDevs[i]);
                            if (i<23) row.append(",");
                        }
                        return new Tuple2<String, String>(row.toString(), null);
                    }
                }).saveAsNewAPIHadoopFile(outputPath, Text.class, NullWritable.class, TextOutputFormat.class);

        sc.stop();
    }

    public static void main(String[] args) {
        String usage = "java ca.uwaterloo.iss4e.DataPreparator InputPath OutputPath";
        if (args.length != 2) {
            System.out.println(usage);
        } else {
            DataPreparator.train(args[0], args[1]);
        }
    }
}
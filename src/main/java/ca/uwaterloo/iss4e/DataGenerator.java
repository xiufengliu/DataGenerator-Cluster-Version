package ca.uwaterloo.iss4e;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by xiuli on 2/22/16.
 */
public class DataGenerator {
    private static final Logger log = Logger.getLogger(DataGenerator.class);
    private static  Random fRandom = new Random();

    static final double[][] slopes = {
            {(float) -0.0059, (float) 0.0155, (float) 0.2233},
            {(float) -0.0136, (float) -0.0049, (float) 0.0315},
            {(float) -0.0079, (float) 0.0112, (float) 0.1365},
            {(float) -0.0146, (float) 0.0034, (float) 0.0774}};

    public static double getMinValue(double[] array) {
        double minValue = array[0];
        for (int i = 1; i < array.length; i++) {
            if (array[i] < minValue) {
                minValue = array[i];
            }
        }
        return minValue;
    }

    public static double getMean(double[] data) {
        double sum = 0.0;
        for (double a : data)
            sum += a;
        return sum / data.length;
    }

    public static double getStdDev(double[] data) {
        double mean = getMean(data);
        double temp = 0;
        for (double a : data)
            temp += (mean - a) * (mean - a);
        return Math.sqrt(temp / data.length);
    }


    public static double getGaussian(double aMean, double stdDev){
        return aMean + fRandom.nextGaussian() * stdDev;
    }

    public static double[] loadsByTemperature(final double[] temperatures, final double[] slopes) {
        double[] loads = new double[temperatures.length];
        Random ran = new Random();
        double X0 = getMinValue(temperatures);
        double Y0 = (double) Math.abs(ran.nextGaussian() / 1.5);
        int X1 = ran.nextInt(6) + 10;
        int X2 = X1 + 4 + ran.nextInt(9);

        for (int i = 0; i < temperatures.length; ++i) {
            double X = temperatures[i];
            double Y = 0.0;
            if (X < X1) {
                Y = slopes[0] * (X - X0) + Y0; // Section1
            } else if (X < X2) {
                Y = slopes[1] * (X - X1) + (slopes[0] * (X1 - X0) + Y0); // Section2
            } else {
                Y = slopes[2] * (X - X2) + slopes[1] * (X2 - X1)
                        + (slopes[0] * (X1 - X0) + Y0); // Section3
            }

            Y=Y<0.0?0.0:Y;
            loads[i] = Y;
            loads[i] = Y > 0.2 ? 0.2 : Y; // The maximum load contributed by
            // temperature is not greater than
            // 0.2kW
        }
        return loads;
    }

    public static String getDate(String  startDate, int nDay) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        Date date = format.parse(startDate);
        cal.setTime(date);
        cal.add(Calendar.DATE, nDay);

        return format.format(cal.getTime());

    }

    public static void generate(int parallelism, String readingSeedInputPath, String temperatureSeedInputPath, String outputPath, final String startDate, final int nDays, int numberOfTimeseries, final double peakyRatio) {
        SparkConf conf = new SparkConf().setAppName("Smart Meter Data Generator");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        final int partitionSize = numberOfTimeseries / parallelism;
        sc.broadcast(partitionSize);
        sc.broadcast(startDate);
        sc.broadcast(nDays);
        sc.broadcast(peakyRatio);

        JavaRDD<String> readingSeedLines = sc.textFile(readingSeedInputPath);
        final Map<Integer, double[][]> activityReadingStdSeeds = readingSeedLines
                .mapToPair(new PairFunction<String, Integer, double[][]>() {
                    @Override
                    public Tuple2<Integer, double[][]> call(String line) throws Exception {
                        String[] fields = line.split("\\|");
                        int meterID = Integer.valueOf(fields[0]);
                        String[] readingArray = fields[1].split(",");
                        String[] stdDevArray = fields[2].split(",");
                        double[][] readingStdDevs = new double[2][24];
                        for (int i = 0; i < 24; ++i) {
                            readingStdDevs[0][i] = Double.parseDouble(readingArray[i]);
                            readingStdDevs[1][i] = Double.parseDouble(stdDevArray[i]);
                        }
                        return new Tuple2<Integer, double[][]>(meterID, readingStdDevs);
                    }
                })
               .collectAsMap();
        sc.broadcast(activityReadingStdSeeds);

        JavaRDD<String> temperatureSeedLines = sc.textFile(temperatureSeedInputPath);
        final List<double[]> temperatureSeeds = temperatureSeedLines.map(new Function<String, double[]>() {
            @Override
            public double[] call(String line) throws Exception {
                String[] fields = line.split("\\|");
                double[] temperatures = new double[fields.length];
                for (int i = 0; i < fields.length; ++i) {
                    temperatures[i] = Double.parseDouble(fields[i]);
                }
                return temperatures;
            }
        }).collect();
        final double[] temperatureSeed = temperatureSeeds.get(0);
        final double tempratureStdDev = 2.0;
        sc.broadcast(temperatureSeed);
        sc.broadcast(tempratureStdDev);

        sc.emptyRDD()
                .repartition(parallelism)
                .mapPartitionsWithIndex(new Function2<Integer, Iterator<Object>, Iterator<Integer>>() {
                    @Override
                    public Iterator<Integer> call(Integer ind, Iterator<Object> objectIterator) throws Exception {
                        List<Integer> IDs = new ArrayList<Integer>();
                        for (int n = 0; n < partitionSize; ++n) {
                            int ID = ind * partitionSize + n;
                            IDs.add(ID);
                        }
                        return IDs.iterator();
                    }
                }, true)
                .flatMapToPair(new PairFlatMapFunction<Integer, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(Integer ID) throws Exception {
                        List<Tuple2<String, String>> lines = new ArrayList<Tuple2<String, String>>();
                        List<Integer> meterIDs = new ArrayList<Integer>(activityReadingStdSeeds.keySet());
                        int meterID = meterIDs.get(fRandom.nextInt(meterIDs.size()));
                        double[][] activityReadingStdSeed = activityReadingStdSeeds.get(meterID);

                        double[] synTemperatures = new double[nDays*24];
                        for (int i = 0; i < nDays*24; ++i) {
                            synTemperatures[i] = getGaussian(temperatureSeed[i % temperatureSeed.length], tempratureStdDev);
                        }

                        double[] loadDueToTemp = loadsByTemperature(synTemperatures, slopes[0]);
                        for (int nDay = 0; nDay < nDays; ++nDay) {
                            double[]synLoad = new double[24];
                            double[]synActivityloads = new double[24];
                            double sum = 0.0;
                            for (int hour = 0; hour < 24; ++hour) {
                                //double synDayActivityReading = getGaussian(activityReadingStdSeed[0][hour], activityReadingStdSeed[1][hour]);
                                double actualActivityReading = activityReadingStdSeed[0][hour];
                                double synDayActivityReading = getGaussian(actualActivityReading, 0.1);
                                if (synDayActivityReading < 0.0) synDayActivityReading = 0.0;
                                synLoad[hour] = synDayActivityReading + loadDueToTemp[nDay * 24 + hour];
                                synActivityloads[hour] = synDayActivityReading;
                                sum += synLoad[hour];
                            }

                            double avgLoad = sum/24.0;
                            for (int hour = 0; hour < 24; ++hour) {
                                double diff = synLoad[hour]-avgLoad;
                                double synPeakLoad = diff>0.0?(avgLoad+diff*peakyRatio):synLoad[hour];
                                //lines.add(new Tuple2<String, String>(String.format("%d|%d|%s %s:00:00|%f|%f|%f|%f|%f", ID, meterID, getDate(startDate, nDay), hour, synLoad[hour], synPeakLoad, synActivityloads[hour], activityReadingStdSeed[0][hour], synTemperatures[nDay * 24 + hour]), null));
                                lines.add(new Tuple2<String, String>(String.format("%d|%s %s:00:00|%f|%f", ID, getDate(startDate, nDay), hour, synPeakLoad, synTemperatures[nDay * 24 + hour]), null));
                            }
                        }
                        return lines;
                    }
                }).saveAsNewAPIHadoopFile(outputPath, Text.class, NullWritable.class, TextOutputFormat.class);
        sc.stop();
    }


    public static void main(String[] args) {
        String usage = "java ca.uwaterloo.iss4e.DataPreparator parallelism activityReadingSeedInputPath temperatureSeedInputPath outputPath startDate nDays numberOfTimeseries peakyRatio";
        if (args.length != 8) {
            System.out.println(usage);
        } else {
            int parallelism = Integer.parseInt(args[0]);
            String activityReadingSeedInputPath = args[1];
            String temperatureSeedInputPath = args[2];
            String outputPath = args[3];
            String startDate = args[4];
            int nDays = Integer.parseInt(args[5]);
            int numberOfTimeseries = Integer.parseInt(args[6]);
            double peakyRatio = Double.parseDouble(args[7]);
            DataGenerator.generate(parallelism, activityReadingSeedInputPath, temperatureSeedInputPath, outputPath, startDate, nDays, numberOfTimeseries, peakyRatio);
        }
    }
}

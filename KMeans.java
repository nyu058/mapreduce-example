package my.kmean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class KMeans {

    public static class PointsMapper extends Mapper<LongWritable, Text, Text, Text> {

        // centroids : Linked-list/arraylike

        private ArrayList<Point2D.Double> centers = new ArrayList<>();

        @Override
        public void setup(Mapper.Context context) throws IOException, InterruptedException {
//			int k = Integer.parseInt(context.getConfiguration().get("k"));

            super.setup(context);
            Configuration conf = context.getConfiguration();

            // retrive file path
            Path centroids = new Path(conf.get("centroid.path"));

            // create a filesystem object
            FileSystem fs = FileSystem.get(conf);

            // create a file reader
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids, conf);

            // read centroids from the file and store them in a centroids variable
            Text key = new Text();
            IntWritable value = new IntWritable();
            while (reader.next(key, value)) {
                String[] pointStr = key.toString().split(",");
                double x = Double.parseDouble(pointStr[0]);
                double y = Double.parseDouble(pointStr[1]);
                Point2D.Double point = new Point2D.Double(x, y);
                centers.add(point);
            }
            reader.close();

        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Double minDistance = Double.MAX_VALUE;
            Integer closetIdx = -1;
            String[] pointStr = value.toString().split(",");
            double x = Double.parseDouble(pointStr[0]);
            double y = Double.parseDouble(pointStr[1]);
            Point2D.Double point = new Point2D.Double(x, y);
            for (var center : this.centers) {
                double currDistance = center.distance(point);
                if (currDistance < minDistance) {
                    closetIdx = centers.indexOf(center);
                    minDistance = currDistance;
                }
            }

            context.write(new Text(closetIdx.toString()), value);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

        }

    }

    public static class PointsReducer extends Reducer<Text, Text, Text, Text> {

        // new_centroids (variable to store the new centroids
        @Override
        public void setup(Context context) {

        }

        public static double getMean(ArrayList<Double> list) {
            double averageValue = 0;
            double sum = 0;

            if (list.size() > 0) {
                for (var elem : list) {

                    sum += elem;
                }
                averageValue = (sum / (double) list.size());
            }

            return averageValue;
        }


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // setup data structure for calculation of mean
            ArrayList<Double> x = new ArrayList<>();
            ArrayList<Double> y = new ArrayList<>();

            for (var value : values) {
                String[] pointStr = value.toString().split(",");
                if (pointStr.length < 2) {
                    continue;
                }
                x.add(Double.parseDouble(pointStr[0]));
                y.add(Double.parseDouble(pointStr[1]));

            }
            double meanX = getMean(x);
            double meanY = getMean(y);
            context.write(new Text((double) Math.round(meanX * 10) / 10 + "," + (double) Math.round(meanY * 10) / 10), new Text("0"));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
//			 BufferedWriter
//			 delete the old centroids
//			 write the new centroids

        }

    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        ArrayList<String> initVal = new ArrayList<>();
        initVal.add("50.197031637442876,32.94048164287042");
        initVal.add("43.407412339767056,6.541037020010927");
        initVal.add("1.7885358732482017,19.666057053079573");
        initVal.add("32.6358540480337,4.03843047564191");
        initVal.add("11.946765766150154,9.908840910435895");
        initVal.add("12.860156581388527,7.5980566765931385");

        System.out.println("Starting Kmean!");

        Configuration conf = new Configuration();

        Path center_path = new Path("centroid/cen.seq");
        conf.set("centroid.path", center_path.toString());

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(center_path)) {
            fs.delete(center_path, true);
        }

        final SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, center_path, Text.class,
                IntWritable.class);
        final IntWritable value = new IntWritable(0);

        // pick any number less than k<6, init base on this vaule
        int k = Integer.parseInt(args[2]);
        for(int i =0;i<k;i++){
            centerWriter.append(new Text(initVal.get(i)), value);
        }

        centerWriter.close();
        int itr = 0;
        ArrayList<Point2D.Double> prevCenters = new ArrayList<>();
        while (true) {
            ArrayList<Point2D.Double> newCenters = new ArrayList<>();
            itr++;
            Job job = Job.getInstance(conf, "kmean " + itr);
            job.setJarByClass(KMeans.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(PointsMapper.class);
            job.setReducerClass(PointsReducer.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            Path output = new Path(args[1] + "/tmp");
            FileOutputFormat.setOutputPath(job, output);
            job.waitForCompletion(true);

            // create a file reader
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(output.toString() + "/part-r-00000"))));
            Text key = new Text();
            System.out.println("New centers:");
            String line = reader.readLine();
            while (line != null) {
                System.out.println(line);
                String[] pointStr = line.split("\\s+")[0].split(",");
                double x = Double.parseDouble(pointStr[0]);
                double y = Double.parseDouble(pointStr[1]);
                Point2D.Double point = new Point2D.Double(x, y);
                newCenters.add(point);
                line = reader.readLine();
            }
            reader.close();

            final SequenceFile.Writer centerWriterPost = SequenceFile.createWriter(fs, conf, center_path, Text.class,
                    IntWritable.class);
            for (var center : newCenters) {
                centerWriterPost.append(new Text(center.getX() + "," + center.getY()), value);
            }

            centerWriterPost.close();
            fs.delete(output);
            // STOP CONDITION
            if (prevCenters.equals(newCenters) || itr >= 10) {
                break;
            }
            prevCenters.clear();
            prevCenters.addAll(newCenters);
            // read the centroid file from hdfs and print the centroids (final result)

        }
        System.out.println("Final centers:");
        System.out.println(prevCenters);
        System.out.println("Run ended after " + itr + " iterations with K = " + args[2]);
        long endTime = System.currentTimeMillis();
        System.out.println("The run took " + (endTime - startTime)/1000 + " seconds");
    }
}
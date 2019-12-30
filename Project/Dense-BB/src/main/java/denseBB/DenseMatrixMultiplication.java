package denseBB;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class DenseMatrixMultiplication extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(DenseMatrixMultiplication.class);

    public static class DenseBBMapper extends Mapper<Object, Text, Text, Text> {

        int leftHorizontalPartitions;
        int leftVerticalPartitions;
        int rightVerticalPartitions;

        private final String ROW_DELIMITER = ",";
        private final String COL_DELIMITER = ":";
        private final String KEY_VALUE_DELIMITER = ",";
        private final String LEFT_MATRIX_ID = "L";
        private final String RIGHT_MATRIX_ID = "R";


        @Override
        public void setup(Context context) {

            leftHorizontalPartitions = Integer.parseInt(context.getConfiguration().get("leftHorizontalPartitions"));
            leftVerticalPartitions = Integer.parseInt(context.getConfiguration().get("leftVerticalPartitions"));
            rightVerticalPartitions = Integer.parseInt(context.getConfiguration().get("rightVerticalPartitions"));
        }

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String[] matrixRecord = value.toString().split(ROW_DELIMITER);
            String matrixId = matrixRecord[0];
            int rowId = Integer.parseInt(matrixRecord[1]) - 1;

            Text outputKey = new Text();
            Text outputValue = new Text();

            if (matrixId.equals(LEFT_MATRIX_ID)) {
                int I = rowId % leftHorizontalPartitions;
                for (int c = 2; c < matrixRecord.length; c++) {
                    int columnId = c - 2;
                    int cellValue = Integer.parseInt(matrixRecord[c]);

                    int J = columnId % leftVerticalPartitions;

                    for (int k = 0; k < rightVerticalPartitions; k++) {
                        outputKey.set(I + KEY_VALUE_DELIMITER + J + KEY_VALUE_DELIMITER + k);
                        outputValue.set(matrixId + KEY_VALUE_DELIMITER + rowId + KEY_VALUE_DELIMITER + columnId + KEY_VALUE_DELIMITER + cellValue);
                        context.write(outputKey, outputValue);
                    }
                }
            } else if (matrixId.equals(RIGHT_MATRIX_ID)) {
                int J = rowId % leftVerticalPartitions;
                for (int c = 2; c < matrixRecord.length; c++) {
                    int columnId = c - 2;
                    int cellValue = Integer.parseInt(matrixRecord[c]);

                    int K = columnId % rightVerticalPartitions;

                    for (int i = 0; i < leftHorizontalPartitions; i++) {
                        outputKey.set(i + KEY_VALUE_DELIMITER + J + KEY_VALUE_DELIMITER + K);
                        outputValue.set(matrixId + KEY_VALUE_DELIMITER + rowId + KEY_VALUE_DELIMITER + columnId + KEY_VALUE_DELIMITER + cellValue);
                        context.write(outputKey, outputValue);
                    }
                }
            }
        }
    }

    public static class PartialSumMapper extends Mapper<Object, Text, Text, Text> {

        private final String KEY_VALUE_DELIMITER = ",";

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final String[] tokens = value.toString().trim().split(KEY_VALUE_DELIMITER);
            String rowId = tokens[0];
            String columnId = tokens[1];
            int partialSumValue = Integer.parseInt(tokens[2]);
            Text outputKey = new Text();
            Text outputValue = new Text();
            outputKey.set(rowId);
            outputValue.set(columnId + KEY_VALUE_DELIMITER + partialSumValue);
            context.write(outputKey, outputValue);
        }
    }


    public static class DenseBBReducer extends Reducer<Text, Text, NullWritable, Text> {

        private final String KEY_VALUE_DELIMITER = ",";

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            Map<Integer, Map<Integer, Integer>> leftRowsToColsMap = new HashMap<>();
            Map<Integer, Map<Integer, Integer>> rightColsToRowsMap = new HashMap<>();

            for (Text value : values) {

                // Split input value into matrixId,rowId,colId,cellValue
                String[] tokens = value.toString().split(KEY_VALUE_DELIMITER);
                String matrixId = tokens[0];
                int rowId = Integer.parseInt(tokens[1]);
                int columnId = Integer.parseInt(tokens[2]);
                int cellValue = Integer.parseInt(tokens[3]);

                if (matrixId.equals("R")) {
                    Map colToRowMap = rightColsToRowsMap.getOrDefault(columnId, new HashMap<>());
                    colToRowMap.put(rowId, cellValue);
                    rightColsToRowsMap.put(columnId, colToRowMap);
                } else {
                    Map rowToColMap = leftRowsToColsMap.getOrDefault(rowId, new HashMap<>());
                    rowToColMap.put(columnId, cellValue);
                    leftRowsToColsMap.put(rowId, rowToColMap);
                }
            }

            for (int i : leftRowsToColsMap.keySet()) {
                for (int k : rightColsToRowsMap.keySet()) {
                    int sum = 0;
                    for (int j : rightColsToRowsMap.get(k).keySet()) {
                        sum += leftRowsToColsMap.get(i).get(j) * rightColsToRowsMap.get(k).get(j);
                    }
                    context.write(NullWritable.get(), new Text((i + 1) + KEY_VALUE_DELIMITER + (k + 1) + KEY_VALUE_DELIMITER + sum));
                }
            }

        }
    }


    public static class PartialSumReducer extends Reducer<Text, Text, NullWritable, Text> {

        private final String KEY_VALUE_DELIMITER = ",";
        private final String COLUMN_DELIMITER = ":";


        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            Map<String, Long> columnIdToSumMap = new HashMap<>();
            String rowId = key.toString();
            for (Text value : values) {
                String[] columnPartialSumPair = value.toString().split(KEY_VALUE_DELIMITER);
                String columnId = columnPartialSumPair[0];
                long partialSum = Long.parseLong(columnPartialSumPair[1]);
                columnIdToSumMap.put(columnId, columnIdToSumMap.getOrDefault(columnId, 0L) + partialSum);
            }

            StringBuilder outputValue = new StringBuilder();
            outputValue.append(rowId);
            for (Map.Entry<String, Long> entry : columnIdToSumMap.entrySet()) {
                outputValue.append(KEY_VALUE_DELIMITER);
                outputValue.append(entry.getKey());
                outputValue.append(COLUMN_DELIMITER);
                outputValue.append(entry.getValue());
            }
            context.write(NullWritable.get(), new Text(outputValue.toString()));
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Dense BB Job 1: calculating partial sums");
        job.setJarByClass(DenseMatrixMultiplication.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        jobConf.set("leftHorizontalPartitions", args[5]);
        jobConf.set("leftVerticalPartitions", args[6]);
        jobConf.set("rightVerticalPartitions", args[7]);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, DenseBBMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DenseBBMapper.class);
        FileOutputFormat.setOutputPath(job, new Path("partial_sum"));

        job.setReducerClass(DenseBBReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);


        Job job2 = Job.getInstance(conf, "Dense BB Job 2: adding partial sums");
        job2.setJarByClass(DenseMatrixMultiplication.class);

        FileInputFormat.addInputPath(job2, new Path("partial_sum"));
        FileOutputFormat.setOutputPath(job2, new Path(args[8]));

        job2.setMapperClass(PartialSumMapper.class);
        job2.setReducerClass(PartialSumReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 9) {
            throw new Error("Nine arguments required:");
        }

        try {
            ToolRunner.run(new DenseMatrixMultiplication(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}
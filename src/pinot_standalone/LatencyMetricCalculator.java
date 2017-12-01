package pinot_standalone;

import java.io.*;
import java.util.ArrayList;
import java.util.List;



public class LatencyMetricCalculator {

    final static int LATENCY_POS = 2;
    final static int DOC_SCANNED_POS = 5;
    final static int QPS_POS = 1;
    final static int SEGMENT_SIZE_POS = 3;
    final static int SEGMENT_COUNT_POS = 4;
    final static double ALPHA = 1;
    final static double BETA = 1;



    public static void findTableBasedAverages(String tableName, List<String> files, String outFile) {

        if (files.isEmpty()) {
            return;
        }
        String line = null;
        String cvsSplitBy  = ",";
        double avg_latency = 0.0;
        double min_latency = Double.MAX_VALUE;
        double max_latency = Double.MIN_VALUE;
        double avg_doc_scanned = 0.0;
        double min_doc_scanned =Double.MAX_VALUE;
        double max_doc_scanned =Double.MIN_VALUE;;
        double avg_qps = 0.0;
        double avg_seg_size = 0.0;
        double avg_seg_count = 0.0;
        int no_of_records = 0;
        for (String file : files) {
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {

                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                    no_of_records++;
                    String[] logLine = line.split(cvsSplitBy);
                    if (min_latency > Double.parseDouble(logLine[LATENCY_POS])){
                        min_latency = Double.parseDouble(logLine[LATENCY_POS]);
                    }
                    if (max_latency < Double.parseDouble(logLine[LATENCY_POS])){
                        max_latency = Double.parseDouble(logLine[LATENCY_POS]);
                    }

                    avg_latency += Double.parseDouble(logLine[LATENCY_POS]);

                    avg_doc_scanned += Double.parseDouble(logLine[DOC_SCANNED_POS]);
                    if (min_doc_scanned > Double.parseDouble(logLine[DOC_SCANNED_POS])){
                        min_doc_scanned = Double.parseDouble(logLine[DOC_SCANNED_POS]);
                    }
                    if (max_doc_scanned < Double.parseDouble(logLine[DOC_SCANNED_POS])){
                        max_doc_scanned = Double.parseDouble(logLine[DOC_SCANNED_POS]);
                    }
                    avg_qps += Double.parseDouble(logLine[QPS_POS]);
                    avg_seg_size += Double.parseDouble(logLine[SEGMENT_SIZE_POS]);
                    avg_seg_count += Double.parseDouble(logLine[SEGMENT_COUNT_POS]);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if(no_of_records>0){
            avg_latency = avg_latency/no_of_records;
            avg_qps = avg_qps/no_of_records;
            avg_doc_scanned = avg_doc_scanned/no_of_records;
            avg_seg_count = avg_seg_count/no_of_records;
            avg_seg_size = avg_seg_size/no_of_records;
            double cost = calculateCost(avg_latency,min_latency,max_latency,avg_doc_scanned,min_doc_scanned,max_doc_scanned,avg_qps);
            String logLine = tableName+","+avg_seg_size+","+cost;
            writeToFile(outFile,logLine);
        }

    }

    public static double calculateCost(double avg_latency,double min_lat,double max_lat,double avg_doc_scanned,double min_doc_scanned,double maxdoc_scanned,double qps){
        double normalized_lat = (avg_latency - min_lat)/(max_lat-min_lat);
        System.out.println(normalized_lat);
        double normalized_doc_scanned = (avg_doc_scanned - min_doc_scanned)/(maxdoc_scanned-min_doc_scanned);
        System.out.println(normalized_doc_scanned);
        double cost  =  (ALPHA*normalized_lat + BETA*normalized_doc_scanned)*qps;
        return cost;
    }

    public static void  writeToFile(String filePath, String logLine){
        try(FileWriter fw = new FileWriter("myfile.txt", true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw))
        {
            out.println(logLine);
        } catch (IOException e) {
            //exception handling left as an exercise for the reader
        }
    }
}


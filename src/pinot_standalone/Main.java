package pinot_standalone;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

public class Main {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        /*

        while (true) {
            System.out.println("Do you want to read remote logs. Type Y or N ");
            String readLogs = scanner.nextLine();
            if (readLogs.equalsIgnoreCase("Y")) {
                *//*System.out.println("Enter the server address");
                String serverAddr = scanner.nextLine();
                System.out.println("Enter user name ");
                String usr = scanner.nextLine();
                System.out.println("Enter the password");
                String passwd = scanner.nextLine();
                System.out.println("Enter the remote dir");
                String remoteDir = scanner.nextLine();
                System.out.println("Enter the output dir");
                String outDir = scanner.nextLine();
                System.out.println("Enter path of key if required or enter N");
                String key = scanner.nextLine();*//*
                System.out.println("Enter the server config file path");
                String serverFile = scanner.nextLine();
                System.out.println("Enter the Output Dir");
                String outDir = scanner.nextLine();
                RemoteLogCollector.fetchlogs(serverFile, outDir);
                //getRemoteLogs(serverAddr, usr, passwd, remoteDir,outDir,key);
            } else {
                break;
            }
            //writeJSONToFile(metrics);
        }*/
        System.out.println("Enter the logs directory based on which you want to calculate cost");
        String path = scanner.next();
        System.out.println("Enter the table names config file");
        String job_path = scanner.next();
        System.out.println("Enter the Output directory for results");
        String cost_dir = scanner.next();
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();
        String line = null;
        List<String> jobs = new ArrayList<>();
        Map<String, List<String>> container = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(job_path))) {
            while ((line = br.readLine()) != null) {
                container.put(line, new ArrayList<String>());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                if(container.containsKey(listOfFiles[i].getName().split("_")[0]))
                    container.get(listOfFiles[i].getName().split("_")[0]).add(listOfFiles[i].getAbsolutePath());
                System.out.println("File " + listOfFiles[i].getName());
            }
        }

        //list.add("D:/l1.txt");
        //list.add("D:/l2.txt");
        for (String key : container.keySet()) {
            if(!container.get(key).isEmpty())
                LatencyMetricCalculator.findTableBasedAverages(key,container.get(key),cost_dir);
        }
    }
}

package pinot_standalone;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;
import java.util.Scanner;

public class RemoteLogCollector {

    /**
     *  api to fetch remote logs.
     * @param srvrSSH
     * @param userSSH
     * @param pswdSSH
     * @param dirPath
     * @param key
     * @param outDir
     */
    public static void getRemoteLogs(String srvrSSH, String userSSH, String pswdSSH, String dirPath,String key,String outDir) {
        com.jcraft.jsch.Session session = null;
        Channel channel = null;
        //String privateKey = "pace_cloud.pem";
        try {
            JSch ssh = new JSch();
            if (! key.equalsIgnoreCase("N"))
                ssh.addIdentity(key);
            JSch.setConfig("StrictHostKeyChecking", "no");
            session = ssh.getSession(userSSH, srvrSSH, 22);
            if(pswdSSH.length()>0)
                session.setPassword(pswdSSH);
            session.connect();
            channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftp = (ChannelSftp) channel;
            sftp.cd(dirPath);
            List<ChannelSftp.LsEntry> list = sftp.ls("*");
            for (ChannelSftp.LsEntry entry : list) {
                sftp.get(entry.getFilename(),outDir+entry.getFilename());
            }
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            if (channel != null) {
                channel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }

    }

    public static void fetchlogs(String configFile, String outDir,String keyPath){
        String line = null;
        try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {

            while ((line = br.readLine()) != null) {
               String[] info = line.split(",");
                getRemoteLogs(info[0], info[1], info[2], info[3],keyPath,outDir);
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void main(String args[]){
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("Do you want to read remote logs. Type Y or N ");
            String readLogs = scanner.nextLine();
            if (readLogs.equalsIgnoreCase("Y")) {
                /*System.out.println("Enter the server address");
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
                String key = scanner.nextLine();*/
                System.out.println("Enter the server config file path");
                String serverFile = scanner.nextLine();
                System.out.println("Enter the Output Dir");
                String outDir = scanner.nextLine();
                System.out.println("Enter the public key file path");
                String keyPath = scanner.nextLine();
                fetchlogs(serverFile,outDir,keyPath);
                //getRemoteLogs(serverAddr, usr, passwd, remoteDir,outDir,key);
            } else {
                break;
            }
            //writeJSONToFile(metrics);
        }
    }
}





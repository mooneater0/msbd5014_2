package org.ip.flink.util;

import java.io.*;
import java.util.ArrayList;

public class dataProcessing {
    public static void main(String[] args) {
        String filePathInput = "/Users/wuyue/Documents/HKUST/5014_2/msbd5014_2/input/originalData/";

        String insertPrefix = "insert>";
        String deletePrefix = "delete>";
        String[] filenames = {"customer.tbl", "lineitem.tbl", "nation.tbl", "orders.tbl", "region.tbl", "supplier.tbl"};
        int[] fileLineCounts = {15000, 600572, 25, 150000, 5, 1000};

        // Create inserting data
        for (String filename : filenames) {
            String filePathOutput = "/Users/wuyue/Documents/HKUST/5014_2/msbd5014_2/input/processedData/insert/";
            try {
                BufferedReader reader = new BufferedReader(new FileReader(filePathInput + filename));
                StringBuilder stringBuilder = new StringBuilder();
                String line;

                while ((line = reader.readLine()) != null) {
                    stringBuilder.append(insertPrefix).append(line).append("\n");
                }

                reader.close();

                BufferedWriter writer = new BufferedWriter(new FileWriter(filePathOutput + filename));
                writer.write(stringBuilder.toString());

                writer.close();

                System.out.println("指定字符串已添加到文件每行前面！");
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        // Create deleting data
        for (int i = 0; i < filenames.length; i++) {
            ArrayList<String> deleteLines = new ArrayList<>();
            String filename = filenames[i];
            int deleteLineCount = fileLineCounts[i] / 2;
            String filePathOutput = "/Users/wuyue/Documents/HKUST/5014_2/msbd5014_2/input/processedData/delete/";
            try {
                BufferedReader reader = new BufferedReader(new FileReader(filePathInput + filename));
                StringBuilder stringBuilder = new StringBuilder();
                String line;
                int curLineCount = 1;

                while ((line = reader.readLine()) != null) {
                    if (curLineCount <= deleteLineCount) {
                        deleteLines.add(line);
                    }
                    curLineCount++;
                }

                reader.close();
                System.out.println(filename);
                System.out.println(fileLineCounts[i]);
                System.out.println(deleteLineCount);

                for (String deleteLine : deleteLines) {
                    stringBuilder.append(deletePrefix).append(deleteLine).append('\n');
                }

                BufferedWriter writer = new BufferedWriter(new FileWriter(filePathOutput + filename));
                writer.write(stringBuilder.toString());

                writer.close();

            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
}

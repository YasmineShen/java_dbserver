package edu.uob;

import java.io.*;
import java.util.*;


public class Database {
    String name;
    Map<String, Table> tables = new HashMap<>();

    Database(String name) {
        this.name = name;
    }

    // Retrieves a table by name
    Table getTable(String tableName) {
        return tables.get(tableName.toLowerCase());
    }

    void saveTableToFile(Table table, String dbRootPath) {
        File dbFolder = new File(dbRootPath, name);
        // Ensure the database folder exists
        if(!dbFolder.exists()) {
            dbFolder.mkdir();
        }
        File tableFile = new File(dbFolder, table.name.toLowerCase() + ".tab");
        try(PrintWriter pw = new PrintWriter(new FileWriter(tableFile, false))) {
            List<String> cols = table.getColumns();
            // Write column headers
            pw.println(String.join("\t", cols));

            for(Row r : table.rows) {
                StringBuilder sb = new StringBuilder();
                // First column is the row ID
                sb.append(r.id);
                for(int i=1; i<cols.size(); i++){
                    String cName = cols.get(i).toLowerCase();
                    String val = r.values.getOrDefault(cName, "");
                    sb.append("\t").append(val);
                }
                // Write row data
                pw.println(sb.toString());
            }
        } catch(IOException e) {
            System.err.println("Failed to save table to file: " + tableFile.getAbsolutePath());
        }
    }

    void loadTableFromFile(File tableFile) {
        String rawName = tableFile.getName();
        String tName = rawName.substring(0, rawName.length()-4);
        Table tbl = null;
        try(BufferedReader br = new BufferedReader(new FileReader(tableFile))) {
            String header = br.readLine();
            if(header == null) {
                // If the file is empty, create a table with no columns
                tbl = new Table(tName, new ArrayList<>());
            }
            else {
                // Split header row to get column names
                String[] colArr = header.split("\t");
                List<String> loadedCols = Arrays.asList(colArr);
                tbl = new Table(tName, loadedCols.subList(1, loadedCols.size()), true);

                String line;
                while((line = br.readLine()) != null) {
                    // Split row data
                    String[] parts = line.split("\t", -1);
                    if(parts.length != colArr.length) {
                        throw new IOException("Row column count mismatch");
                    }
                    int rid = Integer.parseInt(parts[0]);
                    Row row = new Row(rid);
                    for(int i=1; i<colArr.length; i++){
                        String cName = colArr[i].toLowerCase();
                        row.values.put(cName, parts[i]);
                    }
                    tbl.rows.add(row);
                    if(rid >= tbl.nextId) {
                        // Ensure nextId is correct
                        tbl.nextId = rid + 1;
                    }
                }
            }
            // Store table in the database
            tables.put(tName.toLowerCase(), tbl);
        } catch(IOException e) {
            System.err.println("Failed to load table from file: " + tableFile.getAbsolutePath());
        }
    }
}

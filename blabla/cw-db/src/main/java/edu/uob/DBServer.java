package edu.uob;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** This class implements the DB server. */
public class DBServer {

    private static final char END_OF_TRANSMISSION = 4;
    private String storageFolderPath;

    public static void main(String args[]) throws IOException {
        DBServer server = new DBServer();
        server.blockingListenOn(8888);
    }

    /**
     * KEEP this signature otherwise we won't be able to mark your submission correctly.
     */
    public DBServer() {
        storageFolderPath = Paths.get("databases").toAbsolutePath().toString();
        try {
            // Create the database storage folder if it doesn't already exist !
            Files.createDirectories(Paths.get(storageFolderPath));
        } catch(IOException ioe) {
            System.out.println("Can't seem to create database storage folder " + storageFolderPath);
        }
        importAllDatabases();
    }

    private final Map<String, Database> databases = new HashMap<>();
    private String currentDatabaseName = null;

    public void blockingListenOn(int portNumber) throws IOException {
        try (ServerSocket s = new ServerSocket(portNumber)) {
            System.out.println("Server listening on port " + portNumber);
            while (!Thread.interrupted()) {
                try {
                    blockingHandleConnection(s);
                } catch (IOException e) {
                    System.err.println("Server encountered a non-fatal IO error:");
                    e.printStackTrace();
                    System.err.println("Continuing...");
                }
            }
        }
    }

    private void blockingHandleConnection(ServerSocket serverSocket) throws IOException {
        try (Socket s = serverSocket.accept();
             BufferedReader reader = new BufferedReader(new InputStreamReader(s.getInputStream()));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()))) {

            System.out.println("Connection established: " + serverSocket.getInetAddress());
            while (!Thread.interrupted()) {
                String incomingCommand = reader.readLine();
                System.out.println("Received message: " + incomingCommand);
                String result = handleCommand(incomingCommand);
                writer.write(result);
                writer.write("\n" + END_OF_TRANSMISSION + "\n");
                writer.flush();
            }
        }
    }

    /**
     * KEEP this signature (i.e. {@code edu.uob.DBServer.handleCommand(String)})
     * otherwise we won't be able to mark your submission correctly.
     */
    public String handleCommand(String command) {
        if(command == null || command.trim().isEmpty()) {
            return "[ERROR] Empty or null command";
        }
        command = command.trim();
        if(!command.endsWith(";")) {
            return "[ERROR] Semicolon is missing at the end of the line";
        }
        command = command.substring(0, command.length() - 1).trim();

        String result;
        try {
            result = parseAndRunCommand(command);
        } catch (Exception e) {
            result = "[ERROR] " + e.getMessage();
        }
        return result;
    }

    private String parseAndRunCommand(String command) {
        // 1) USE <dbname>
        Pattern usePattern = Pattern.compile("(?i)^USE\\s+([A-Za-z0-9]+)$");
        Matcher useMatcher = usePattern.matcher(command);
        if(useMatcher.find()) {
            String dbName = useMatcher.group(1);
            return toCreateUse(dbName);
        }

        // 2) CREATE DATABASE <dbname>
        Pattern createDBPattern = Pattern.compile("(?i)^CREATE\\s+DATABASE\\s+([A-Za-z0-9]+)$");
        Matcher createDBMatcher = createDBPattern.matcher(command);
        if(createDBMatcher.find()) {
            String dbName = createDBMatcher.group(1);
            return toCreateDatabase(dbName);
        }

        // 3) CREATE TABLE <tableName>
        Pattern createTableWithColsPattern = Pattern.compile("(?i)^CREATE\\s+TABLE\\s+([A-Za-z0-9]+)\\s*\\((.*)\\)$");
        Matcher createTableWithColsMatcher = createTableWithColsPattern.matcher(command);
        if(createTableWithColsMatcher.find()) {
            String tableName = createTableWithColsMatcher.group(1);
            String colsRaw = createTableWithColsMatcher.group(2).trim();
            return toCreateTable(tableName, colsRaw);
        }

        Pattern createTableNoColsPattern = Pattern.compile("(?i)^CREATE\\s+TABLE\\s+([A-Za-z0-9]+)$");
        Matcher createTableNoColsMatcher = createTableNoColsPattern.matcher(command);
        if(createTableNoColsMatcher.find()) {
            String tableName = createTableNoColsMatcher.group(1);
            return toCreateTable(tableName, "");
        }

        // 4) DROP DATABASE <dbname>
        Pattern dropDBPattern = Pattern.compile("(?i)^DROP\\s+DATABASE\\s+([A-Za-z0-9]+)$");
        Matcher dropDBMatcher = dropDBPattern.matcher(command);
        if(dropDBMatcher.find()) {
            String dbName = dropDBMatcher.group(1);
            return toDropDatabase(dbName);
        }

        Pattern dropTablePattern = Pattern.compile("(?i)^DROP\\s+TABLE\\s+([A-Za-z0-9]+)$");
        Matcher dropTableMatcher = dropTablePattern.matcher(command);
        if(dropTableMatcher.find()) {
            String tableName = dropTableMatcher.group(1);
            return toDropTable(tableName);
        }

        // 5) ALTER TABLE <tableName> ADD <colName> or ALTER TABLE <tableName> DROP <colName>
        Pattern alterPattern = Pattern.compile("(?i)^ALTER\\s+TABLE\\s+([A-Za-z0-9]+)\\s+(ADD|DROP)\\s+([A-Za-z0-9]+)$");
        Matcher alterMatcher = alterPattern.matcher(command);
        if(alterMatcher.find()) {
            String tableName = alterMatcher.group(1);
            String action = alterMatcher.group(2).toUpperCase();
            String colName = alterMatcher.group(3);
            return toAlterTable(tableName, action, colName);
        }

        // 6) INSERT INTO <table> VALUES(...)
        Pattern insertPattern = Pattern.compile("(?i)^INSERT\\s+INTO\\s+([A-Za-z0-9]+)\\s+VALUES\\s*\\((.*)\\)$");
        Matcher insertMatcher = insertPattern.matcher(command);
        if(insertMatcher.find()) {
            String tableName = insertMatcher.group(1);
            String valuesRaw = insertMatcher.group(2).trim();
            return toInsert(tableName, valuesRaw);
        }

        // 7) SELECT <columns> FROM <table> [WHERE <condition>]
        Pattern selectPattern = Pattern.compile("(?i)^SELECT\\s+(.*)\\s+FROM\\s+([A-Za-z0-9]+)(?:\\s+WHERE\\s+(.*))?$");
        Matcher selectMatcher = selectPattern.matcher(command);
        if(selectMatcher.find()) {
            String colsPart = selectMatcher.group(1).trim();
            String tableName = selectMatcher.group(2);
            String wherePart = selectMatcher.group(3);
            if(wherePart != null) {
                wherePart = wherePart.trim();
            }
            return toSelect(tableName, colsPart, wherePart);
        }

        // 8) UPDATE <table> SET <nameValueList> WHERE <condition>
        Pattern updatePattern = Pattern.compile("(?i)^UPDATE\\s+([A-Za-z0-9]+)\\s+SET\\s+(.*)\\s+WHERE\\s+(.*)$");
        Matcher updateMatcher = updatePattern.matcher(command);
        if(updateMatcher.find()) {
            String tableName = updateMatcher.group(1);
            String setPart = updateMatcher.group(2).trim();
            String wherePart = updateMatcher.group(3).trim();
            return toUpdate(tableName, setPart, wherePart);
        }

        // 9) DELETE FROM <table> WHERE <condition>
        Pattern deletePattern = Pattern.compile("(?i)^DELETE\\s+FROM\\s+([A-Za-z0-9]+)\\s+WHERE\\s+(.*)$");
        Matcher deleteMatcher = deletePattern.matcher(command);
        if(deleteMatcher.find()) {
            String tableName = deleteMatcher.group(1);
            String wherePart = deleteMatcher.group(2).trim();
            return toDelete(tableName, wherePart);
        }

        // 10) JOIN <table1> AND <table2> ON <col1> AND <col2>
        Pattern joinPattern = Pattern.compile("(?i)^JOIN\\s+([A-Za-z0-9]+)\\s+AND\\s+([A-Za-z0-9]+)\\s+ON\\s+([A-Za-z0-9]+)\\s+AND\\s+([A-Za-z0-9]+)$");
        Matcher joinMatcher = joinPattern.matcher(command);
        if(joinMatcher.find()) {
            String table1 = joinMatcher.group(1);
            String table2 = joinMatcher.group(2);
            String col1 = joinMatcher.group(3);
            String col2 = joinMatcher.group(4);
            return toJoin(table1, table2, col1, col2);
        }

        return "[ERROR] Command not recognized or invalid syntax";
    }

    private String toCreateUse(String dbName) {
        String dbLower = dbName.toLowerCase();
        if(!databases.containsKey(dbLower)) {
            return "[ERROR] Database does not exist: " + dbName;
        }
        currentDatabaseName = dbLower;
        return "[OK]";
    }

    private String toCreateDatabase(String dbName) {
        String dbLower = dbName.toLowerCase();
        if(databases.containsKey(dbLower)) {
            return "[ERROR] Database already exists: " + dbName;
        }

        File dbFolder = new File(storageFolderPath, dbLower);
        if(dbFolder.exists()) {
            return "[ERROR] Folder already exists: " + dbName;
        }
        boolean success = dbFolder.mkdir();
        if(!success) {
            return "[ERROR] Failed to create database folder for: " + dbName;
        }
        Database dbObj = new Database(dbLower);
        databases.put(dbLower, dbObj);
        return "[OK]";
    }

    private String toCreateTable(String tableName, String colsRaw) {
        if(currentDatabaseName == null) {
            return "[ERROR] No database selected.";
        }
        Database db = databases.get(currentDatabaseName);
        if(db == null) {
            return "[ERROR] No database exists";
        }

        String tblLower = tableName.toLowerCase();
        if(db.tables.containsKey(tblLower)) {
            return "[ERROR] Table already exists: " + tableName;
        }

        List<String> userDefinedCols = new ArrayList<>();
        if(!colsRaw.isEmpty()) {
            String[] arr = colsRaw.split(",");
            for(String c : arr) {
                userDefinedCols.add(c.trim());
            }
        }
        Table newTable = new Table(tableName, userDefinedCols);
        db.tables.put(tblLower, newTable);

        db.saveTableToFile(newTable, storageFolderPath);
        return "[OK]";
    }

    private String toDropDatabase(String dbName) {
        String dbLower = dbName.toLowerCase();
        if(!databases.containsKey(dbLower)) {
            return "[ERROR] Database not found: " + dbName;
        }
        databases.remove(dbLower);

        File dbFolder = new File(storageFolderPath, dbLower);
        if(dbFolder.exists()) {
            deleteDirectory(dbFolder);
        }

        if(currentDatabaseName != null && currentDatabaseName.equals(dbLower)) {
            currentDatabaseName = null;
        }
        return "[OK]";
    }


    private String toDropTable(String tableName) {
        if(currentDatabaseName == null) {
            return "[ERROR] No database selected.";
        }
        Database db = databases.get(currentDatabaseName);
        if(db == null) {
            return "[ERROR] No database exists";
        }
        String tblLower = tableName.toLowerCase();
        if(!db.tables.containsKey(tblLower)) {
            return "[ERROR] Table does not exist: " + tableName;
        }
        db.tables.remove(tblLower);
        File tableFile = new File(new File(storageFolderPath, currentDatabaseName), tblLower + ".tab");
        if(tableFile.exists()) {
            boolean deleted = tableFile.delete();
            if(!deleted) {
                return "[ERROR] Failed to delete table file: " + tableName;
            }
        }
        return "[OK]";
    }

    private String toAlterTable(String tableName, String action, String colName) {
        if(currentDatabaseName == null) {
            return "[ERROR] No database selected.";
        }
        Database db = databases.get(currentDatabaseName);
        if(db == null) {
            return "[ERROR] No database exists";
        }
        Table table = db.getTable(tableName);
        if(table == null) {
            return "[ERROR] Table not found: " + tableName;
        }

        String colNameLower = colName.toLowerCase();
        if("ADD".equals(action)) {

            if(table.hasColumn(colNameLower)) {
                return "[ERROR] Column already exists: " + colName;
            }

            table.addColumn(colName);
        }
        else if("DROP".equals(action)) {
            if(colNameLower.equals("id")) {
                return "[ERROR] Cannot drop 'id' column.";
            }
            if(!table.hasColumn(colNameLower)) {
                return "[ERROR] Column not found: " + colName;
            }
            table.dropColumn(colName);
        }
        else {
            return "[ERROR] Invalid ALTER action: " + action;
        }

        db.saveTableToFile(table, storageFolderPath);
        return "[OK]";
    }

    private String toInsert(String tableName, String valuesRaw) {
        if(currentDatabaseName == null) {
            return "[ERROR] No database selected.";
        }
        Database db = databases.get(currentDatabaseName);
        if(db == null) {
            return "[ERROR] No database exists";
        }
        Table table = db.getTable(tableName);
        if(table == null) {
            return "[ERROR] Table not found: " + tableName;
        }
        List<String> insertedValues = parseValueList(valuesRaw);
        int expected = table.getColumns().size() - 1;
        if(insertedValues.size() != expected) {
            return "[ERROR] Inserted values count mismatch. Expect " + expected;
        }

        Row newRow = new Row(table.generateNextId());
        for(int i=1; i<table.getColumns().size(); i++){
            String colName = table.getColumns().get(i);
            newRow.values.put(colName.toLowerCase(), insertedValues.get(i-1));
        }
        table.rows.add(newRow);

        db.saveTableToFile(table, storageFolderPath);
        return "[OK]";
    }

    private String toSelect(String tableName, String colPart, String wherePart) {
        if(currentDatabaseName == null) {
            return "[ERROR] No database selected.";
        }
        Database db = databases.get(currentDatabaseName);
        if(db == null) {
            return "[ERROR] No database exists";
        }
        Table table = db.getTable(tableName);
        if(table == null) {
            return "[ERROR] Table not found: " + tableName;
        }

        List<String> selectedCols;
        if(colPart.trim().equals("*")) {
            selectedCols = new ArrayList<>(table.getColumns());
        }
        else {
            selectedCols = parseAttributeList(colPart);
            for(String col : selectedCols) {
                if(!table.hasColumn(col.toLowerCase())) {
                    return "[ERROR] Column not found: " + col;
                }
            }
        }

        List<Row> matchedRows = table.filterRows(wherePart);
        if(matchedRows.isEmpty()) {
            return "[ERROR] No matching rows found.";
        }

        StringBuilder sb = new StringBuilder("[OK]\n");

        for(String col : selectedCols) {
            sb.append(col).append("\t");
        }
        sb.append("\n");

        for(Row r : matchedRows) {
            for(String col : selectedCols) {
                String colLower = col.toLowerCase();
                if(colLower.equals("id")) {
                    sb.append(r.id).append("\t");
                } else {
                    String val = r.values.getOrDefault(colLower, "");
                    sb.append(val).append("\t");
                }
            }
            sb.append("\n");
        }
        return sb.toString().trim();
    }

    private String toUpdate(String tableName, String setPart, String wherePart) {
        if(currentDatabaseName == null) {
            return "[ERROR] No database selected.";
        }
        Database db = databases.get(currentDatabaseName);
        if(db == null) {
            return "[ERROR] No database exists";
        }
        Table table = db.getTable(tableName);
        if(table == null) {
            return "[ERROR] Table not found: " + tableName;
        }

        Map<String, String> setMap = parseNameValueList(setPart);

        for(String colKey : setMap.keySet()) {
            if(colKey.equalsIgnoreCase("id")) {
                return "[ERROR] Cannot update 'id' column.";
            }
            if(!table.hasColumn(colKey)) {
                return "[ERROR] Column not found: " + colKey;
            }
        }

        List<Row> matchedRows = table.filterRows(wherePart);
        if(matchedRows.isEmpty()) {
            return "[ERROR] No matching rows found for update.";
        }

        for(Row row : matchedRows) {
            for(Map.Entry<String,String> e : setMap.entrySet()) {
                row.values.put(e.getKey().toLowerCase(), e.getValue());
            }
        }

        db.saveTableToFile(table, storageFolderPath);
        return "[OK]";
    }

    private String toDelete(String tableName, String wherePart) {
        if(currentDatabaseName == null) {
            return "[ERROR] No database selected.";
        }
        Database db = databases.get(currentDatabaseName);
        if(db == null) {
            return "[ERROR] No database exists";
        }
        Table table = db.getTable(tableName);
        if(table == null) {
            return "[ERROR] Table not found: " + tableName;
        }

        List<Row> matchedRows = table.filterRows(wherePart);
        if(matchedRows.isEmpty()) {
            return "[ERROR] No matching rows found for delete.";
        }

        table.rows.removeAll(matchedRows);

        db.saveTableToFile(table, storageFolderPath);
        return "[OK]";
    }

    private String toJoin(String table1, String table2, String col1, String col2) {
        if(currentDatabaseName == null) {
            return "[ERROR] No database selected.";
        }
        Database db = databases.get(currentDatabaseName);
        if(db == null) {
            return "[ERROR] No database exists";
        }
        Table t1 = db.getTable(table1);
        Table t2 = db.getTable(table2);
        if(t1 == null || t2 == null) {
            return "[ERROR] One or both tables do not exist";
        }

        if(!t1.hasColumn(col1.toLowerCase()) || !t2.hasColumn(col2.toLowerCase())) {
            return "[ERROR] Join column not found in table(s)";
        }

        List<Row> resultRows = new ArrayList<>();
        for(Row r1 : t1.rows) {
            String v1 = r1.values.getOrDefault(col1.toLowerCase(), "");
            for(Row r2 : t2.rows) {
                String v2 = r2.values.getOrDefault(col2.toLowerCase(), "");
                if(v1.equals(v2)) {

                    Row joined = new Row(generateTempId());

                    for(int i=1; i<t1.getColumns().size(); i++){
                        String cName = t1.getColumns().get(i);
                        String val = r1.values.getOrDefault(cName.toLowerCase(), "");
                        joined.values.put(t1.name.toLowerCase()+"."+cName.toLowerCase(), val);
                    }

                    for(int i=1; i<t2.getColumns().size(); i++){
                        String cName = t2.getColumns().get(i);
                        String val = r2.values.getOrDefault(cName.toLowerCase(), "");
                        joined.values.put(t2.name.toLowerCase()+"."+cName.toLowerCase(), val);
                    }
                    resultRows.add(joined);
                }
            }
        }

        List<String> joinedCols = new ArrayList<>();
        joinedCols.add("id");

        for(int i=1; i<t1.getColumns().size(); i++){
            joinedCols.add(t1.name+"."+t1.getColumns().get(i));
        }

        for(int i=1; i<t2.getColumns().size(); i++){
            joinedCols.add(t2.name+"."+t2.getColumns().get(i));
        }

        StringBuilder sb = new StringBuilder("[OK]\n");
        for(String c : joinedCols) {
            sb.append(c).append("\t");
        }
        sb.append("\n");
        for(Row r : resultRows) {
            sb.append(r.id).append("\t");
            for(int i=1; i<joinedCols.size(); i++){
                String colNameFull = joinedCols.get(i).toLowerCase();
                String val = r.values.getOrDefault(colNameFull, "");
                sb.append(val).append("\t");
            }
            sb.append("\n");
        }
        return sb.toString().trim();
    }

    private void importAllDatabases() {
        File dbRoot = new File(storageFolderPath);
        File[] dbFolders = dbRoot.listFiles();
        if(dbFolders == null) return;
        for(File folder : dbFolders) {
            if(folder.isDirectory()) {
                String dbNameLower = folder.getName().toLowerCase();
                Database dbObj = new Database(dbNameLower);
                File[] tableFiles = folder.listFiles();
                if(tableFiles != null) {
                    for(File tf : tableFiles) {
                        if(tf.isFile() && tf.getName().endsWith(".tab")) {
                            dbObj.loadTableFromFile(tf);
                        }
                    }
                }
                databases.put(dbNameLower, dbObj);
            }
        }
    }

    private List<String> parseValueList(String raw) {
        List<String> result = new ArrayList<>();
        String[] arr = raw.split(",");
        for(String s : arr) {
            s = s.trim();

            if(s.startsWith("'") && s.endsWith("'") && s.length() > 1) {
                s = s.substring(1, s.length()-1);
            }
            result.add(s);
        }
        return result;
    }

    private List<String> parseAttributeList(String raw) {
        List<String> result = new ArrayList<>();
        String[] arr = raw.split(",");
        for(String s : arr) {
            result.add(s.trim());
        }
        return result;
    }

    private Map<String,String> parseNameValueList(String raw) {
        Map<String,String> map = new LinkedHashMap<>();
        String[] pairs = raw.split(",");
        for(String p : pairs) {
            p = p.trim();

            String[] kv = p.split("=");
            if(kv.length != 2) {
                throw new RuntimeException("Invalid SET syntax near: " + p);
            }
            String key = kv[0].trim().toLowerCase();
            String val = kv[1].trim();

            if(val.startsWith("'") && val.endsWith("'") && val.length()>1) {
                val = val.substring(1, val.length()-1);
            }
            map.put(key, val);
        }
        return map;
    }

    private void deleteDirectory(File file) {
        if(file.isDirectory()){
            File[] children = file.listFiles();
            if(children != null){
                for(File c : children){
                    deleteDirectory(c);
                }
            }
        }
        boolean ok = file.delete();
        if(!ok){
            System.err.println("[ERROR] Failed to delete: " + file.getAbsolutePath());
        }
    }

    private static int tempIdCounter = 100000000;
    private synchronized int generateTempId() {
        return tempIdCounter++;
    }
}
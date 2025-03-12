package edu.uob;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class Table {
    final String name;
    final List<String> columns;
    final List<Row> rows = new ArrayList<>();
    int nextId = 1;

    Table(String name, List<String> userDefinedColumns) {
        this.name = name;
        this.columns = new ArrayList<>();
        // ID column is always present
        this.columns.add("id");
        for(String c : userDefinedColumns) {
            this.columns.add(c);
        }
    }

    Table(String name, List<String> loadedCols, boolean fromFile) {
        this.name = name;
        this.columns = new ArrayList<>();
        this.columns.add("id");
        this.columns.addAll(loadedCols);
    }

    // Returns the list of column names
    List<String> getColumns(){
        return columns;
    }

    // Checks if a column exists in the table
    boolean hasColumn(String colLower){
        for(String c : columns) {
            if(c.equalsIgnoreCase(colLower)) {
                return true;
            }
        }
        return false;
    }
    int generateNextId(){
        return nextId++;
    }

    void addColumn(String colName) {
        columns.add(colName);
        for(Row r : rows) {
            r.values.put(colName.toLowerCase(), "");
        }
    }

    void dropColumn(String colName) {
        for(int i=0; i<columns.size(); i++){
            if(columns.get(i).equalsIgnoreCase(colName)) {
                columns.remove(i);
                break;
            }
        }
        for(Row r : rows){
            r.values.remove(colName.toLowerCase());
        }
    }

    List<Row> filterRows(String wherePart){
        if(wherePart == null || wherePart.trim().isEmpty()){
            return new ArrayList<>(rows);
        }
        Pattern p = Pattern.compile("(?i)^([A-Za-z0-9]+)\\s*==\\s*'(.*)'$");
        Matcher m = p.matcher(wherePart.trim());
        if(!m.find()) {
            return new ArrayList<>();
        }
        String col = m.group(1).toLowerCase();
        String val = m.group(2);

        List<Row> matched = new ArrayList<>();
        for(Row r : rows) {
            String leftVal = r.values.getOrDefault(col, "");
            if(leftVal.equals(val)) {
                matched.add(r);
            }
        }
        return matched;
    }
}

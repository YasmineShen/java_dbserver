package edu.uob;

import java.util.HashMap;
import java.util.Map;


public class Row {
    final int id;
    final Map<String,String> values = new HashMap<>();

    Row(int id) {
        this.id = id;
    }
}

package org.example.config;

import com.google.gson.Gson;

public class ComputeConf {
    public String type;
    public String name;
    public String sql;
    public String columns;
    public String sources;
    public ComputeConf() {}
    public ComputeConf(String type, String name, String sql, String columns, String sources) {
        this.type = type;
        this.name = name;
        this.sql = sql;
        this.columns = columns;
        this.sources = sources;
    }
    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}

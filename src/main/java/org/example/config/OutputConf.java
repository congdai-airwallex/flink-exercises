package org.example.config;

import com.google.gson.Gson;

public class OutputConf {
    public String sql;
    public String sinks;
    public OutputConf() {}
    public OutputConf(String sql, String sinks) {
        this.sql = sql;
        this.sinks = sinks;
    }
    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}

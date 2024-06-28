package org.example.config;

import com.google.gson.Gson;

public class SourceConf {
    public String name;
    public String type;
    public SourceConf() {};
    public SourceConf(String name, String type) {
        this.name = name;
        this.type = type;
    }
    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}

package org.example.config;

import com.google.gson.Gson;

import java.util.List;

public class FeatureConf {
    public String name;
    public String owner;
    public List<SourceConf> sources;
    public List<ComputeConf> compute;
    public List<OutputConf> output;
    public FeatureConf() {}
    public FeatureConf(String name, String owner, List<SourceConf> sources, List<ComputeConf> compute, List<OutputConf> output) {
        this.name = name;
        this.owner = owner;
        this.sources = sources;
        this.compute = compute;
        this.output = output;
    }
    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}

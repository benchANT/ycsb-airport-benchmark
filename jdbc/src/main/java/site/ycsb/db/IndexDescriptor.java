package site.ycsb.db;

import java.util.ArrayList;
import java.util.List;

public final class IndexDescriptor {
    public String name;
    public String method;
    public String order;
    public boolean concurrent;
    public List<String> columnNames = new ArrayList<>();
}
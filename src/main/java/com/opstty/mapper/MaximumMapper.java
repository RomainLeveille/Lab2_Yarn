package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MaximumMapper extends Mapper<Object, Text, Text, Writable> {
    private Text word = new Text();
    private IntWritable nb = new IntWritable();
    private NullWritable nw = NullWritable.get();
    private String head = "GEOPOINT;ARRONDISSEMENT;GENRE;ESPECE;FAMILLE;ANNEE PLANTATION;HAUTEUR;CIRCONFERENCE;ADRESSE;NOM COMMUN;VARIETE;OBJECTID;NOM_EV";

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        
        if(value.toString().equals(head))
                return;
        String[] my_tree = (value.toString()).split(";");
        String str = my_tree[2];
        int flt = (int) Double.parseDouble(my_tree[6]);

        try {
            word.set(str);
            nb.set(flt);
            context.write(word, nb);
        } catch (Exception e) {
            word.set(str);
            context.write(word, nw);
        }
    }   

}

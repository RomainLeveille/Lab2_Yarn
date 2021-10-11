package com.opstty;

import com.opstty.job.WordCount;
import com.opstty.job.Districts_trees;
import com.opstty.job.Existing_species;
import com.opstty.job.Kind_trees;
import com.opstty.job.Maximum_height;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districts_trees", Districts_trees.class,
                    "A map/reduce program that display the list of distincts containing trees.");
            programDriver.addClass("existing_species", Existing_species.class,
                    "A map/reduce program that display the list of distincts species trees.");
            programDriver.addClass("kind_trees", Kind_trees.class,
                    "A map/reduce program that display the list of distincts kind of trees.");
            programDriver.addClass("maximum_height", Maximum_height.class,
                    "A map/reduce program that display the taller tree of each kind.");
            

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}

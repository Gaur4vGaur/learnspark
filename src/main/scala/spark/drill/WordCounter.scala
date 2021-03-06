package spark.drill;

import org.apache.spark.SparkContext;

/**
 * The program to count the number of words showed up in input.
 *
 * @author Gaurav Gaur
 *
 **/

object WordCounter {

    def main(args: Array[String]): Unit = {
        val inpath = "input/wiki.txt"
        val outpath = "target/output/wordcount"

        val sc = new SparkContext("local[*]", "Word Count")

        try {
            val input = sc.textFile(inpath)
            val wc = input.map(_.toLowerCase).
                    flatMap(txt => txt.split("""\W+""")).
                    groupBy(word => word).
                    mapValues(group => group.size)//.sortBy(_._2, false) sort descending

            println(s"writing output")
            wc.saveAsTextFile(outpath)
        } finally {
            sc.stop()
        }
    }
}
package spark.drill;

import org.apache.spark.SparkContext;

/**
 * The program to count the number of words showed up in input.
 * The program does not use any shuffeling and hence is more efficient.
 *
 * @author Gaurav Gaur
 *
 **/
object WordCounterOptimized {

    def main(args: Array[String]): Unit = {
        val inpath = "input/wiki.txt"
        val outpath = "output/wordcount"

        val sc = new SparkContext("local[*]", "Word Count")

        try {
            val input = sc.textFile(inpath)
            val wc = input.map(_.toLowerCase).
                    flatMap(txt => txt.split("""\W+""")).
                    map(word => (word, 1)).
                    reduceByKey((n1, n2) => n1 + n2)

            println(s"writing output")
            wc.saveAsTextFile(outpath)
        } finally {
            sc.stop()
        }
    }
}
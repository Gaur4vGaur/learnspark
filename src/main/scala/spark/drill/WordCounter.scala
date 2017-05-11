package spark.drill;

import org.apache.spark.SparkContext;

object WordCounter {

    def main(args: Array[String]): Unit = {
        val inpath = "/home/gaurav/1My/study/myprojects/spark/BDU/spark-foundation1/LabData/notebook.log"
        val outpath = "output/wordcount"

        val sc = new SparkContext("local[*]", "Word Count")

        try {
            val input = sc.textFile(inpath)
            val wc = input.map(_.toLowerCase).
                    flatMap(txt => txt.split("""\W+""")).
                    groupBy(word => word).
                    mapValues(group => group.size)

            println(s"writing output")
            wc.saveAsTextFile(outpath)
        } finally {
            sc.stop()
        }
    }
}
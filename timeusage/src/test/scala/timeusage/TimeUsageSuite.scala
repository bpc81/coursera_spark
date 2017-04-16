package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {
  import TimeUsage._

  val resource = "/timeusage/atussum.csv"

  test("reading data file schema") {
    val rdd = spark.sparkContext.textFile(fsPath(resource))

    val headerColumns = rdd.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(headerColumns)

    println(schema)
  }

  test("reading data file") {
    val (columns, initDf) = read(resource)
    val numCols = columns.length
    assert(numCols === 455)
    println(s"Number of Columns: $numCols")
    //println(columns.length)
    //initDf.printSchema()
    //initDf.show(10)
    val numRows = initDf.count()
    assert(numRows === 170842)
    println(s"Number of rows: $numRows")
  }

  test("classified columns") {
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    assert(primaryNeedsColumns.length === 55)
    assert(workColumns.length === 23)
    assert(otherColumns.length === 432)
    println("Counting columns:")
    println(s"  Primary needs: ${primaryNeedsColumns.length}")
    println(s"  Work: ${workColumns.length}")
    println(s"  Other: ${otherColumns.length}")
  }

  test("summary dataset") {
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    summaryDf.printSchema()
    summaryDf.show(10)
  }
}

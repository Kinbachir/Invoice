import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, explode, expr, lit, monotonically_increasing_id, sum}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

import javax.swing.table.{AbstractTableModel}
import javax.swing.{JFrame, JScrollPane, JTable}

object Main extends JFrame {
  def convertDataToArray(collectedData: Array[org.apache.spark.sql.Row]): Array[Array[Any]] = {
    collectedData.map(row => Array(row.getAs[String]("Type"), row.getAs[Long]("InvoiceID"), row.getAs[Double]("Value")))
  }
  class DataFrameTableModel(data: Array[Array[Any]], columns: Array[String]) extends AbstractTableModel {
    override def getRowCount: Int = data.length
    override def getColumnCount: Int = columns.length
    override def getValueAt(rowIndex: Int, columnIndex: Int): AnyRef = data(rowIndex)(columnIndex).asInstanceOf[AnyRef]
    override def getColumnName(column: Int): String = columns(column)
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Complex Types Demo")
      .getOrCreate()

    val schema = StructType(List(
      StructField("InvoiceNumber", StringType),
      StructField("CreatedTime", LongType),
      StructField("StoreID", StringType),
      StructField("PosID", StringType),
      StructField("CashierID", StringType),
      StructField("CustomerType", StringType),
      StructField("CustomerCardNo", StringType),
      StructField("TotalAmount", DoubleType),
      StructField("NumberOfItems", IntegerType),
      StructField("PaymentMethod", StringType),
      StructField("CGST", DoubleType),
      StructField("SGST", DoubleType),
      StructField("CESS", DoubleType),
      StructField("DeliveryType", StringType),
      StructField("DeliveryAddress", StructType(List(
        StructField("AddressLine", StringType),
        StructField("City", StringType),
        StructField("State", StringType),
        StructField("PinCode", StringType),
        StructField("ContactNumber", StringType)
      ))),
      StructField("InvoiceLineItems", ArrayType(StructType(List(
        StructField("ItemCode", StringType),
        StructField("ItemDescription", StringType),
        StructField("ItemPrice", DoubleType),
        StructField("ItemQty", IntegerType),
        StructField("TotalValue", DoubleType),
      )))),
    ))
    val df1 = spark.read.schema(schema).json("data/")
    val dfWithIds = df1.withColumn("InvoiceID", monotonically_increasing_id())
      .withColumn("DeliveryAddressID", monotonically_increasing_id())
    val deliveryAddressDF = dfWithIds.select(
      col("DeliveryAddressID"),
      col("InvoiceID"),
      col("DeliveryAddress.AddressLine"),
      col("DeliveryAddress.City"),
      col("DeliveryAddress.State"),
      col("DeliveryAddress.PinCode"),
      col("DeliveryAddress.ContactNumber")
    )
    val invoiceLineItemsDF = dfWithIds.withColumn("InvoiceLineItems", explode(col("InvoiceLineItems")))
      .select(
        col("InvoiceID"),
        col("InvoiceLineItems.ItemCode"),
        col("InvoiceLineItems.ItemDescription"),
        col("InvoiceLineItems.ItemPrice"),
        col("InvoiceLineItems.ItemQty"),
        col("InvoiceLineItems.TotalValue")
      )
    val invoiceDF = dfWithIds.drop("DeliveryAddress", "InvoiceLineItems")
      .select(
        col("InvoiceID"),
        col("InvoiceNumber"),
        col("CreatedTime"),
        col("StoreID"),
        col("PosID"),
        col("CashierID"),
        col("CustomerType"),
        col("CustomerCardNo"),
        col("TotalAmount"),
        col("NumberOfItems"),
        col("PaymentMethod"),
        col("CGST"),
        col("SGST"),
        col("CESS"),
        col("DeliveryType"),
        col("DeliveryAddressID")
      )
    deliveryAddressDF.write.mode(SaveMode.Overwrite).parquet("output/delivery_address/")
    invoiceLineItemsDF.write.mode(SaveMode.Overwrite).parquet("output/invoice_line_items/")
    invoiceDF.write.mode(SaveMode.Overwrite).parquet("output/invoice/")
    val mostQuantityInvoice = invoiceLineItemsDF.groupBy("InvoiceID")
      .agg(sum("ItemQty").as("TotalQuantity"))
      .orderBy(col("TotalQuantity").desc)
      .limit(1)
      .withColumn("Type", lit("Most Quantity"))
      .withColumn("Value", col("TotalQuantity"))
    val leastQuantityInvoice = invoiceLineItemsDF.groupBy("InvoiceID")
      .agg(sum("ItemQty").as("TotalQuantity"))
      .orderBy(col("TotalQuantity").asc)
      .limit(1)
      .withColumn("Type", lit("Least Quantity"))
      .withColumn("Value", col("TotalQuantity"))
    val highestAmountInvoice = invoiceDF.groupBy("InvoiceID")
      .agg(sum("TotalAmount").as("TotalInvoiceAmount"))
      .orderBy(col("TotalInvoiceAmount").desc)
      .limit(1)
      .withColumn("Type", lit("Highest Amount"))
      .withColumn("Value", col("TotalInvoiceAmount"))
    val lowestAmountInvoice = invoiceDF.groupBy("InvoiceID")
      .agg(sum("TotalAmount").as("TotalInvoiceAmount"))
      .orderBy(col("TotalInvoiceAmount").asc)
      .limit(1)
      .withColumn("Type", lit("Lowest Amount"))
      .withColumn("Value", col("TotalInvoiceAmount"))
    val combinedResults = mostQuantityInvoice
      .union(leastQuantityInvoice)
      .union(highestAmountInvoice)
      .union(lowestAmountInvoice)
      .select("Type", "InvoiceID", "Value")
    val collectedData = combinedResults.collect()
    val data = convertDataToArray(collectedData)
    val columnNames = Array("Type","InvoiceID", "Value")
    val tableModel = new DataFrameTableModel(data, columnNames)
    val table = new JTable(tableModel)
    val scrollPane = new JScrollPane(table)
    val frame = new JFrame("Invoice")
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
    frame.add(scrollPane)
    frame.pack()
    frame.setVisible(true)

    spark.stop()
  }
}

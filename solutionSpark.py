from pyspark.sql import SparkSession
import re
import pyspark.sql.functions as f
from pyspark.sql.functions import when, col, to_date, date_format, count, regexp_replace
from pyspark.sql.types import LongType

spark = SparkSession.builder.appName("TestSession").getOrCreate()


def bankData(ref1):
    df = spark.read.csv(ref1, header=True, inferSchema=True)
    # df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load(ref1)
    return df


def rearrangedCols(ref2):
    df = bankData(ref2).withColumnRenamed('CHQ.NO.', 'CHQ NO')
    for each in df.schema.names:
        df = df.withColumnRenamed(each, re.sub(r'\s+([a-zA-Z_][a-zA-Z_0-9]*)\s*', '', each.replace(' ', '_')))
    df = df.select(df.columns[0:8])
    df = df.withColumn("DATE", to_date(col("DATE"), "dd-MMM-yy"))
    df = df.withColumn("_WITHDRAWAL_AMT_", regexp_replace(col("_WITHDRAWAL_AMT_"), ",", ""))
    df = df.withColumn("_DEPOSIT_AMT_", regexp_replace(col("_DEPOSIT_AMT_"), ",", ""))
    df = df.withColumn("_DEPOSIT_AMT_", df._DEPOSIT_AMT_.cast(LongType()))
    df = df.withColumn("_WITHDRAWAL_AMT_", df._WITHDRAWAL_AMT_.cast(LongType()))
    # df = df.withColumn("DATE", date_format(col("DATE"), "dd-MM-yyyy"))
    # formatting date in 'dd-mm-yyyy' format is possible but, it will change the datatype to string,
    # which will return null values in 'filter' operation
    return df


def addTxType(ref3):
    df = rearrangedCols(ref3)
    df = df.withColumn('TransactionAmount',
                       when(df._WITHDRAWAL_AMT_.isNull(), df._DEPOSIT_AMT_).otherwise(df._WITHDRAWAL_AMT_))
    df = df.withColumn('TransactionType', when(df._WITHDRAWAL_AMT_.isNull(), 'CR').otherwise('DR'))
    df = df[['Account_No', 'DATE', 'TRANSACTION_DETAILS', 'CHQ_NO', 'VALUE_DATE', '_WITHDRAWAL_AMT_', '_DEPOSIT_AMT_',
             'TransactionType', 'TransactionAmount', 'BALANCE_AMT']]
    return df

def txDataByChqNo(ref4):    #step2-1
    df = addTxType(ref4).filter(col("CHQ_NO").isNotNull())
    return df

def txDataByDR(ref5):    #step2-2
    df = addTxType(ref5).filter(col("TransactionType") == 'DR')
    return df

def txDataByCR(ref6):    #step2-3
    df = addTxType(ref6).filter(col("TransactionType") == 'CR')
    return df

def txDataByDateRange(ref7, date_from, date_to):    #step3
    df = addTxType(ref7)
    df = df.filter((col('DATE') >= date_from) & (col('DATE') <= date_to))
    return df

def txDataRemoveDuplicates(ref8):    #step3
    df = addTxType(ref8)
    df = df.groupBy('Account_No', 'DATE', 'TransactionType', 'TransactionAmount').agg(count("*").alias("count"))
    return df

def txDataTotalAmt(ref9, date_from, date_to):    #step4
    df = addTxType(ref9)
    df = df.filter((col('DATE') >= date_from) & (col('DATE') <= date_to))
    pandasDF = df.toPandas()
    pandasDF = pandasDF.groupby(['Account_No'], as_index=False)['_WITHDRAWAL_AMT_', '_DEPOSIT_AMT_'].agg('sum')
    pandasDF.rename(columns={'_WITHDRAWAL_AMT_': 'Total Withdraw', '_DEPOSIT_AMT_': 'Total Deposit'}, inplace=True)
    return pandasDF


def writeCsv(ref10, date_from, date_to):
    df_CsvAddTxType = addTxType(ref10).write.csv('outputPySparkAddTxType', header=True)

    df_CsvTxDataByChqNo = txDataByChqNo(ref10).write.csv('CsvTxDataByChqNo', header=True)
    df_CsvTxDataByDR = txDataByDR(ref10).write.csv('CsvTxDataByDR', header=True)
    df_CsvTxDataByCR = txDataByCR(ref10).write.csv('CsvTxDataByCR', header=True)
    df_CsvTxDataByDate = txDataByDateRange(ref10, date_from, date_to).write.csv('CsvTxDataByDate', header=True)
    df_CsvTxDataRemoveDuplicates = txDataRemoveDuplicates(ref10).write.csv('CsvTxDataRemoveDuplicates', header=True)

    df_txDataTotalAmt = txDataTotalAmt(ref10, date_from, date_to).to_csv('CsvTxDataTotalAmt.csv')

    return df_CsvTxDataByDate, df_CsvAddTxType, df_CsvTxDataByChqNo, df_CsvTxDataByDR, df_CsvTxDataByCR, df_CsvTxDataRemoveDuplicates, df_txDataTotalAmt

def writePqt(ref11):
    df_writePqtTxDataByChqNo = txDataByChqNo(ref11).write.parquet('PqtTxDataByChqNo')
    df_writePqtTxDataByDR = txDataByDR(ref11).write.parquet('PqtTxDataByDR')
    df_writePqtTxDataByCR = txDataByCR(ref11).write.parquet('PqtTxDataByCR')

    return df_writePqtTxDataByChqNo, df_writePqtTxDataByDR, df_writePqtTxDataByCR

writeCsv('sampleBank.csv', '2017-06-12', '2018-09-21')
# writePqt('bank.csv', '2017-06-12', '2018-09-21')

print(txDataByDateRange('sampleBank.csv', '2017-06-12', '2018-09-21'))

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

##excel to pandas dataframe
def bankData(filepath):
    df_bank= pd.read_excel(filepath)
    df_bank= df_bank.drop(df_bank.columns[[8]], axis=1)
    df_bank.rename(columns={'CHQ.NO.': 'CHQ NO'}, inplace=True)
    df_bank.columns = df_bank.columns.str.replace(' ', '')
    return df_bank

##adding TransactionAmount column
def addTxAmt(ref2):
    df_addTxAmt= bankData(ref2)
    df_addTxAmt['TransactionAmount'] = np.where(df_addTxAmt['WITHDRAWALAMT'].isnull(), df_addTxAmt['DEPOSITAMT'], df_addTxAmt['WITHDRAWALAMT'])
    return df_addTxAmt

##adding TransactionsType column
def addTxType(ref3):
    df_addTxType= addTxAmt(ref3)
    df_addTxType['TransactionType'] = np.where(df_addTxType['DEPOSITAMT'].isnull(), 'DR', 'CR')
    return df_addTxType

##reaarange columns & export as csv
def txDataByChqNo(ref4):
    df_bank_final = addTxType(ref4)[['AccountNo', 'DATE', 'TRANSACTIONDETAILS', 'CHQNO', 'VALUEDATE', 'WITHDRAWALAMT', 'DEPOSITAMT', 'TransactionType', 'TransactionAmount', 'BALANCEAMT']]

    df_txDataByChqNo = df_bank_final.loc[df_bank_final['CHQNO'].notnull()]
    df_txDataByChqNo.to_parquet('txDataByChqNo.parquet', engine= 'fastparquet')
    # table = pa.Table.from_pandas(df_txDataByChqNo)
    # pq.write_table(table, 'txDataByChqNoSNAPPY.parquet')
    # pq.write_table(table, 'txDataByChqNoGZIP.parquet', compression='GZIP')
    # pq.write_table(table, 'txDataByChqNoBROTLI.parquet', compression='BROTLI')

    df_txDataByDR = df_bank_final.loc[df_bank_final['TransactionType']== 'DR']
    df_txDataByDR.to_parquet('txDataByDR.parquet')

    df_txDataByCR = df_bank_final.loc[df_bank_final['TransactionType'] == 'CR']
    df_txDataByCR.to_parquet('xDataByCR.parquet')

    return df_txDataByChqNo, df_txDataByDR, df_txDataByCR

txDataByChqNo('sampleBank.xlsx')

# print(txDataByChqNo('sampleBank.xlsx'))
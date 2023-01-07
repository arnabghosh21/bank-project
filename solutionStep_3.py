import pandas as pd
import numpy as np

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
def txDataByChqNo(ref4, Start_date, End_date):
    df_bank_final = addTxType(ref4)[['AccountNo', 'DATE', 'TRANSACTIONDETAILS', 'CHQNO', 'VALUEDATE', 'WITHDRAWALAMT', 'DEPOSITAMT', 'TransactionType', 'TransactionAmount', 'BALANCEAMT']]

    df_txDataByDate = df_bank_final[(df_bank_final['DATE'] >= Start_date) & (df_bank_final['DATE']<= End_date)].reset_index()

    df_txDataByDate.to_parquet('txDataByDate.parquet', engine= 'fastparquet')
    df_txDataByDate.to_csv('txDataByDate.csv')

    return df_txDataByDate

txDataByChqNo('sampleBank.xlsx', '2017-06-12', '2018-09-21')

print(txDataByChqNo('sampleBank.xlsx', '2017-06-12', '2018-09-21'))
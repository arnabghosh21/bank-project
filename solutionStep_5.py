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
def txDataByTotalAmt(ref4):
    df_bank_final = addTxType(ref4)[['AccountNo', 'DATE', 'TRANSACTIONDETAILS', 'CHQNO', 'VALUEDATE', 'WITHDRAWALAMT', 'DEPOSITAMT', 'TransactionType', 'TransactionAmount', 'BALANCEAMT']]

    df_txDataByTotalAmt = df_bank_final.groupby(['AccountNo'], as_index=False)['WITHDRAWALAMT', 'DEPOSITAMT'].agg('sum')
    df_txDataByTotalAmt.rename(columns={'WITHDRAWALAMT': 'Total Withdraw', 'DEPOSITAMT': 'Total Deposit'}, inplace=True)

    df_txDataByTotalAmt.to_csv('txDataByTotalAmt.csv')

    return df_txDataByTotalAmt

txDataByTotalAmt('sampleBank.xlsx')

print(txDataByTotalAmt('sampleBank.xlsx'))
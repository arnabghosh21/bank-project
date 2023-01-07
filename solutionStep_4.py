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
def txDataByChqNo(ref4):
    df_bank_final = addTxType(ref4)[['AccountNo', 'DATE', 'TRANSACTIONDETAILS', 'CHQNO', 'VALUEDATE', 'WITHDRAWALAMT', 'DEPOSITAMT', 'TransactionType', 'TransactionAmount', 'BALANCEAMT']]

    df_txDataRemoveDuplicates = df_bank_final.pivot_table(columns=['AccountNo', 'DATE', 'TransactionType', 'TransactionAmount'],aggfunc='size').to_frame().reset_index()
    # rename column as count
    df_txDataRemoveDuplicates.rename(columns={0: 'count'}, inplace=True)

    df_txDataRemoveDuplicates.to_csv('txDataRemoveDuplicates.csv')

    return df_txDataRemoveDuplicates

txDataByChqNo('sampleBank.xlsx')

print(txDataByChqNo('sampleBank.xlsx'))
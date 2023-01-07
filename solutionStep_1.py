import pandas as pd
import numpy as np

##excel to pandas dataframe
def bankData(filepath):
    df_bank= pd.read_excel(filepath)
    df_bank= df_bank.drop(df_bank.columns[[8]], axis=1)
    return df_bank

##adding TransactionAmount column
def addTxAmt(ref2):
    df_addTxAmt= bankData(ref2)
    df_addTxAmt['TransactionAmount'] = np.where(df_addTxAmt['WITHDRAWAL AMT'].isnull(), df_addTxAmt['DEPOSIT AMT'], df_addTxAmt['WITHDRAWAL AMT'])
    return df_addTxAmt

##adding TransactionsType column
def addTxType(ref3):
    df_addTxType= addTxAmt(ref3)
    df_addTxType['TransactionType'] = np.where(df_addTxType['DEPOSIT AMT'].isnull(), 'DR', 'CR')
    return df_addTxType

##reaarange columns & export as csv
def finalBankData(ref4):
    df_bank_final = addTxType(ref4)[['Account No', 'DATE', 'TRANSACTION DETAILS', 'CHQ.NO.', 'VALUE DATE', 'WITHDRAWAL AMT', 'DEPOSIT AMT', 'TransactionType', 'TransactionAmount', 'BALANCE AMT']]
    df_bank_final.to_csv('output.csv')
    return df_bank_final

finalBankData('bank.xlsx')
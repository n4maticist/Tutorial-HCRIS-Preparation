#%% Import Libraries
import os
import zipfile
import dask.dataframe as dd
import numpy as np
from dask.diagnostics import ProgressBar

#%% Set Paths
input_folder = "data"
output_folder = "output"

#%% Unzip Archives
for file in os.listdir(input_folder):
    filename = os.path.join(input_folder,file)
    if filename.lower().endswith(".zip"):
        with zipfile.ZipFile(os.path.abspath(filename),"r") as zipped:
            zipped.extractall(input_folder)

#%% Setup Columnss
colRPT = ['RPT_REC_NUM','PRVDR_CTRL_TYPE_CD','PRVDR_NUM', 'NPI','RPT_STUS_CD','FY_BGN_DT','FY_END_DT','PROC_DT', \
            'INITL_RPT_SW','LAST_RPT_SW','TRNSMTL_NUM','FI_NUM','ADR_VNDR_CD','FI_CREAT_DT','UTIL_CD','NPR_DT', \
            'SPEC_IND','FI_RCPT_DT']
colNMRC = ['RPT_REC_NUM','WKSHT_CD','LINE_NUM','CLMN_NUM','ITM_VAL']
colALPHA = ['RPT_REC_NUM','WKSHT_CD','LINE_NUM','CLMN_NUM','ITM_VAL']

#%% Read data into Dask dataframe (201X Files)
dfRPT = dd.read_csv(input_folder + '/*_201*_RPT.CSV', header=None, names=colRPT,dtype='str')
dfNMRC = dd.read_csv(input_folder + '/*_201*_NMRC.CSV', header=None, names=colNMRC,dtype='str')
dfALPHA = dd.read_csv(input_folder + '/*_201*_ALPHA.CSV', header=None, names=colALPHA,dtype='str')

#%% Load field descriptors
dfFIELDS = dd.read_csv('https://raw.githubusercontent.com/ohana-project/HCRISFields/master/fields.csv',dtype='str')

#%% Match field data types
dtypes = {
    'WKSHT_CD':np.str,
    'LINE_NUM':np.str,
    'CLMN_NUM':np.str
}
dfNMRC = dfNMRC.astype(dtypes)
dfALPHA = dfALPHA.astype(dtypes)
dfFIELDS = dfFIELDS.astype(dtypes)

#%% Merge Field Information with Numeric Data
dfNMRC = dfNMRC.merge(dfFIELDS, left_on=['WKSHT_CD','LINE_NUM','CLMN_NUM'], \
    right_on=['WKSHT_CD','LINE_NUM','CLMN_NUM'])

#%% Merge Field Information with Alpha Data
dfALPHA = dfALPHA.merge(dfFIELDS, left_on=['WKSHT_CD','LINE_NUM','CLMN_NUM'], \
    right_on=['WKSHT_CD','LINE_NUM','CLMN_NUM'])

#%% Merge Alpha and Numeric
dfNMRC = dfNMRC.merge(dfRPT, left_on='RPT_REC_NUM', right_on='RPT_REC_NUM')
dfALPHA = dfALPHA.merge(dfRPT, left_on='RPT_REC_NUM', right_on='RPT_REC_NUM')

#%% Combine to Final Dataframe
dfFINAL = dfALPHA.append(dfNMRC)

#%% Exporting to Parquet
dfFINAL.to_parquet(output_folder+"mcr", compression={"name": "gzip", "values": "snappy"})

# %%

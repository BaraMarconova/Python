import pandas
# import grow reports
grow_cz = pandas.read_csv(r"C:\Users\bmeu1yzr\OneDrive - Avon\Documents\Grow Leader Queue\data_for_skript\ARP User Report_3_1_2023_CZ.csv",sep=',')
grow_sk = pandas.read_csv(r"C:\Users\bmeu1yzr\OneDrive - Avon\Documents\Grow Leader Queue\data_for_skript\ARP User Report_3_1_2023_SK.csv",sep=',')
# import Al reports
camp_1_cz= pandas.read_csv(r"C:\Users\bmeu1yzr\OneDrive - Avon\Documents\Grow Leader Queue\data_for_skript\CZE_2022_camp12_csv.csv",sep=';',decimal=",")
camp_2_cz= pandas.read_csv(r"C:\Users\bmeu1yzr\OneDrive - Avon\Documents\Grow Leader Queue\data_for_skript\CZE_2023_camp01_csv.csv",sep=';',decimal=",")
camp_3_cz= pandas.read_csv(r"C:\Users\bmeu1yzr\OneDrive - Avon\Documents\Grow Leader Queue\data_for_skript\CZE_2023_camp02_csv.csv",sep=';',decimal=",")
camp_1_sk= pandas.read_csv(r"C:\Users\bmeu1yzr\OneDrive - Avon\Documents\Grow Leader Queue\data_for_skript\SR_2022_camp12_csv.csv",sep=';',decimal=",")
camp_2_sk= pandas.read_csv(r"C:\Users\bmeu1yzr\OneDrive - Avon\Documents\Grow Leader Queue\data_for_skript\SR_2023_camp01_csv.csv",sep=';',decimal=",")
camp_3_sk= pandas.read_csv(r"C:\Users\bmeu1yzr\OneDrive - Avon\Documents\Grow Leader Queue\data_for_skript\SR_2023_camp02_csv.csv",sep=';',decimal=",")


print(grow_sk.head())

grow_cz.rename(columns={'Account Number':'ACCOUNTNUMBER'}, inplace=True)
grow_sk.rename(columns={'Account Number':'ACCOUNTNUMBER'}, inplace=True)
grow_cz.columns = [c.replace(' ', '_') for c in grow_cz.columns]
grow_sk.columns = [c.replace(' ', '_') for c in grow_sk.columns]

#chose needed columns
camp_1_cz = camp_1_cz[['ACCOUNTNUMBER','COORDSEGM','ACTIVEG1','APPOITMENTSG1','NETASALESG1','AWARDSALESG1','LOA1ACTIVEG1','LOA2ACTIVEG1','LOA3ACTIVEG1']]
camp_2_cz = camp_2_cz[['ACCOUNTNUMBER','COORDSEGM','ACTIVEG1','APPOITMENTSG1','NETASALESG1','AWARDSALESG1','LOA1ACTIVEG1','LOA2ACTIVEG1','LOA3ACTIVEG1']]
camp_3_cz = camp_3_cz[['ACCOUNTNUMBER','COORDSEGM','ACTIVEG1','APPOITMENTSG1','NETASALESG1','AWARDSALESG1','LOA1ACTIVEG1','LOA2ACTIVEG1','LOA3ACTIVEG1']]
camp_1_sk = camp_1_sk[['ACCOUNTNUMBER','COORDSEGM','ACTIVEG1','APPOITMENTSG1','NETASALESG1','AWARDSALESG1','LOA1ACTIVEG1','LOA2ACTIVEG1','LOA3ACTIVEG1']]
camp_2_sk = camp_2_sk[['ACCOUNTNUMBER','COORDSEGM','ACTIVEG1','APPOITMENTSG1','NETASALESG1','AWARDSALESG1','LOA1ACTIVEG1','LOA2ACTIVEG1','LOA3ACTIVEG1']]
camp_3_sk = camp_3_sk[['ACCOUNTNUMBER','COORDSEGM','ACTIVEG1','APPOITMENTSG1','NETASALESG1','AWARDSALESG1','LOA1ACTIVEG1','LOA2ACTIVEG1','LOA3ACTIVEG1']]


all_tables_cz = [grow_cz,camp_1_cz,camp_2_cz,camp_3_cz]
all_tables_sk = [grow_sk,camp_1_sk,camp_2_sk,camp_3_sk]

# print(all_tables_sk)
# merge 
import functools as ft
all_tables_cz = ft.reduce( lambda left, right: pandas.merge(left, right, on='ACCOUNTNUMBER', how='left'),all_tables_cz)
all_tables_sk = ft.reduce( lambda left, right: pandas.merge(left, right, on='ACCOUNTNUMBER', how='left'),all_tables_sk)

# change data type for AWARDSALESG1
convert_dict = {'AWARDSALESG1': float,
                'AWARDSALESG1_x': float,
                'AWARDSALESG1_y': float
               }  
all_tables_cz = all_tables_cz.astype(convert_dict)
all_tables_sk = all_tables_sk.astype(convert_dict)

# calculate new columns
all_tables_cz['Appointments_total']=all_tables_cz.APPOITMENTSG1 + all_tables_cz.APPOITMENTSG1_x + all_tables_cz.APPOITMENTSG1_y
all_tables_cz['Average_of_LOA1_LOA3']=((all_tables_cz.LOA1ACTIVEG1+all_tables_cz.LOA2ACTIVEG1+all_tables_cz.LOA3ACTIVEG1)+(all_tables_cz.LOA1ACTIVEG1_x+all_tables_cz.LOA2ACTIVEG1_x+all_tables_cz.LOA3ACTIVEG1_x)+(all_tables_cz.LOA1ACTIVEG1_y+all_tables_cz.LOA2ACTIVEG1_y+all_tables_cz.LOA3ACTIVEG1_y))/3
all_tables_cz['Total_number_of_active_Als']=all_tables_cz.ACTIVEG1+all_tables_cz.ACTIVEG1_x+all_tables_cz.ACTIVEG1_y
all_tables_cz['Average_award_sales_per_active_AL']=(all_tables_cz.AWARDSALESG1+all_tables_cz.AWARDSALESG1_x+all_tables_cz.AWARDSALESG1_y)/(all_tables_cz.Total_number_of_active_Als)
all_tables_sk['Appointments_total']=all_tables_sk.APPOITMENTSG1 + all_tables_sk.APPOITMENTSG1_x + all_tables_sk.APPOITMENTSG1_y
all_tables_sk['Average_of_LOA1_LOA3']=((all_tables_sk.LOA1ACTIVEG1+all_tables_sk.LOA2ACTIVEG1+all_tables_sk.LOA3ACTIVEG1)+(all_tables_sk.LOA1ACTIVEG1_x+all_tables_sk.LOA2ACTIVEG1_x+all_tables_sk.LOA3ACTIVEG1_x)+(all_tables_sk.LOA1ACTIVEG1_y+all_tables_sk.LOA2ACTIVEG1_y+all_tables_sk.LOA3ACTIVEG1_y))/3
all_tables_sk['Total_number_of_active_Als']=all_tables_sk.ACTIVEG1+all_tables_sk.ACTIVEG1_x+all_tables_sk.ACTIVEG1_y
all_tables_sk['Average_award_sales_per_active_AL']=(all_tables_sk.AWARDSALESG1+all_tables_sk.AWARDSALESG1_x+all_tables_sk.AWARDSALESG1_y)/(all_tables_sk.Total_number_of_active_Als)

# # v poslednich 3 campani coordsegm>=11
import numpy as np
all_tables_cz['SL_Confirmed_in_3_campaigns'] = np.where(all_tables_cz['COORDSEGM']>=11 & (all_tables_cz['COORDSEGM_x']>=11) & (all_tables_cz['COORDSEGM_y']>=11),'Yes', 'No')
all_tables_sk['SL_Confirmed_in_3_campaigns'] = np.where(all_tables_sk['COORDSEGM']>=11 & (all_tables_sk['COORDSEGM_x']>=11) & (all_tables_sk['COORDSEGM_y']>=11),'Yes', 'No')

# conditions - drops
all_tables_cz = all_tables_cz.drop(all_tables_cz[all_tables_cz.Zone == 99].index)
all_tables_cz = all_tables_cz.drop(all_tables_cz[all_tables_cz.PRP_Status == 'Disable'].index)
all_tables_cz = all_tables_cz[all_tables_cz['PRP_Status'].notna()]
all_tables_cz = all_tables_cz.drop(all_tables_cz[all_tables_cz.Opt_out == 'Y'].index)
all_tables_cz = all_tables_cz.drop(all_tables_cz[all_tables_cz.Out_of_Office_Enabled == 'Y'].index)
all_tables_cz = all_tables_cz.drop(all_tables_cz[all_tables_cz.SL_Confirmed_in_3_campaigns == 'No'].index)
all_tables_cz = all_tables_cz.drop(all_tables_cz[all_tables_cz.Appointments_total < 5].index)
all_tables_cz = all_tables_cz[all_tables_cz['Appointments_total'].notna()]
all_tables_sk = all_tables_sk.drop(all_tables_sk[all_tables_sk.Zone == 99].index)
all_tables_sk = all_tables_sk.drop(all_tables_sk[all_tables_sk.PRP_Status == 'Disable'].index)
all_tables_sk = all_tables_sk[all_tables_sk['PRP_Status'].notna()]
all_tables_sk = all_tables_sk.drop(all_tables_sk[all_tables_sk.Opt_out == 'Y'].index)
all_tables_sk = all_tables_sk.drop(all_tables_sk[all_tables_sk.Out_of_Office_Enabled == 'Y'].index)
all_tables_sk = all_tables_sk.drop(all_tables_sk[all_tables_sk.SL_Confirmed_in_3_campaigns == 'No'].index)
all_tables_sk = all_tables_sk.drop(all_tables_sk[all_tables_sk.Appointments_total < 5].index)
all_tables_sk = all_tables_sk[all_tables_sk['Appointments_total'].notna()]

# round all numbers to 2 decimels
all_tables_cz = all_tables_cz.round(2)
all_tables_sk = all_tables_sk.round(2)

# create rank columns
all_tables_cz['Rank_average_new_AL'] = all_tables_cz['Average_of_LOA1_LOA3'].rank(method='max',ascending=False)
all_tables_cz['Rank_average_AWS'] = all_tables_cz['Average_award_sales_per_active_AL'].rank(method='max',ascending=False)
all_tables_cz['Score'] = (all_tables_cz.Rank_average_new_AL*0.7) + (all_tables_cz.Rank_average_AWS*0.3)
all_tables_sk['Rank_average_new_AL'] = all_tables_sk['Average_of_LOA1_LOA3'].rank(method='max',ascending=False)
all_tables_sk['Rank_average_AWS'] = all_tables_sk['Average_award_sales_per_active_AL'].rank(method='max',ascending=False)
all_tables_sk['Score'] = (all_tables_sk.Rank_average_new_AL*0.7) + (all_tables_sk.Rank_average_AWS*0.3)

# print(all_tables_sk.shape)
# print(all_tables_sk.shape)

all_tables = pandas.concat([all_tables_cz, all_tables_sk])

print(all_tables.shape)

# export to .csv
all_tables.to_csv('202303_export_ grow_queque_control',sep=',',index=False, encoding = 'utf-8-sig')

# create a file for grow dev team
grow_dev_upload = all_tables[['Market','ACCOUNTNUMBER','Score','Zone']]
grow_dev_upload['Campaign']=202303
grow_dev_upload.to_csv('202303_CZ_SK_Eligible_Sales_Leaders',sep=',',index=False, encoding = 'utf-8-sig')

# create a file to sent to ZM
ZM_grow_report = all_tables[['Market','Zone','ACCOUNTNUMBER','First_Name','Last_Name','Score']]
# create column place - lover score = lower place, lower score is better
ZM_grow_report['Place_in_Zone'] = ZM_grow_report.groupby('Zone')['Score'].rank(method='first', ascending=True)
# create file for ZM report
ZM_grow_report.to_csv('202303_ZM_GROW_REPORT',sep=',',index=False, encoding = 'utf-8-sig')
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 13 10:44:49 2022

@author: spandanrout
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


df=pd.read_csv(r'/Users/spandanrout/Desktop/ZLC/Project_Pfizer/2022_ZLC_Data/20220131_Process orders.csv')
df[['acquisition_ts_est','scheduled_start_datetime','scheduled_finish_datetime']] = df[['acquisition_ts_est','scheduled_start_datetime','scheduled_finish_datetime']].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S.%f')
df = df.sort_values(['process_order', 'acquisition_ts_est'])
df.drop(columns = ['plant_key','resource_desc','planned_order','batch','production_version','created_date'], inplace = True)



#creating a field for measuring the magnitute of change in planned date and checking whenever those changes occcured
df["diff"]=df.groupby('process_order')['scheduled_start_datetime'].diff()
df["diff"] = df["diff"].dt.total_seconds()/60/60
df["diff"] = df["diff"].fillna(0)
df['count'] = np.where(df["diff"]==0,0,1)
first_dates = df.groupby('process_order')['scheduled_start_datetime'].first()
df["Time_Since_First"] = df['scheduled_start_datetime'] - df['process_order'].map(first_dates)
df["Time_Since_First"] = df["Time_Since_First"].dt.total_seconds()/60/60



#removing records that are planned or executed after 3rd of Decemeber because we donot have any
#further supporting data to check when they were actually executed
last_dates = df.groupby('process_order')['scheduled_start_datetime'].last()
last_dates = last_dates.to_frame()
last_dates.reset_index(inplace = True)
last_dates = last_dates.loc[last_dates['scheduled_start_datetime']<= '2021-12-3']
df = pd.merge(df, last_dates[["process_order"]], on = 'process_order', how = 'inner')



#counting & summarising the number of changes for each process order
change_counts  = pd.DataFrame()
change_counts = df.groupby(["process_order"])["count"].sum().reset_index(name="Count")



#Defining & creating new field called Lead_Time in days for analysing the time window that the planner has when they accept an order 
#and compared to when it is scheduled to start
Leadtime = df.groupby('process_order').first()
Leadtime.reset_index(inplace=True)
Leadtime['Lead_Time(D)'] = Leadtime['scheduled_start_datetime']-Leadtime['acquisition_ts_est']
Leadtime['Lead_Time(D)']= Leadtime['Lead_Time(D)'].dt.total_seconds()/60/60/24



#summarising and analysing if there is any relation between number of changes made with the lead time available to the planner
change_counts = pd.merge(change_counts, Leadtime[['process_order','Lead_Time(D)','process_order_item_plan_qty']], on = 'process_order', how = 'left')



#Including Mean & Std_Dev of the time difference and joining the Material Key to analyse whether the magnitude of change 
#is affected by the number of changes being made or whether the leadtime has an affect on the same
result =df.groupby('process_order')['diff'].mean()
result = result.to_frame()
result = result.rename(columns={"diff":"Mean"})
result.reset_index(inplace=True)
result1 =df.groupby('process_order')['diff'].std()
result1 = result1.to_frame()
result1 = result1.rename(columns={"diff":"StdDev"})
result1.reset_index(inplace=True)
result = pd.merge(result, result1[['process_order','StdDev']], on = 'process_order')
change_counts = pd.merge(change_counts, result[['process_order','Mean','StdDev']], on = 'process_order', how = 'left')
change_counts = change_counts.rename(columns={"process_order_item_plan_qty":"Quantity"})



#merging the material keys to the above dataset to check if same material keys behave similarly across different process orders
change_counts = pd.merge(change_counts, df[["process_order","material_key"]], on = 'process_order', how = 'outer')
change_counts = change_counts.drop_duplicates("process_order")



#Including the time diff between last change made and Sch start at that moment
#to capture from how far the planner confidently made the last change
last_change = df.loc[df['count']==1]
last_change = last_change.groupby(['process_order']).last()
last_change.reset_index(inplace=True)
last_change['final_Diff(D)'] = last_change['scheduled_start_datetime']- last_change['acquisition_ts_est']
last_change['final_Diff(D)']= last_change['final_Diff(D)'].dt.total_seconds()/60/60/24
last_change['Day'] = last_change['scheduled_start_datetime'].dt.date



#final dataset containing the count of changes, avg magnitude of change, lead time the planner had when accepted the order
#and how far from executing the order was the last change made
change_counts = pd.merge(change_counts, last_change[['process_order','final_Diff(D)']], on = 'process_order', how = 'left')


'''
corr = change_counts.corr(method = 'spearman')
plt.figure(figsize=(10, 10),dpi = 800)
plt.scatter( change_counts["Lead_Time(D)"],change_counts["Count"], c = "Blue", alpha = 0.8)
plt.xlabel("Lead-Time")
plt.ylabel("Change Frequency")
change_counts.to_csv(r'/Users/spandanrout/Desktop/ZLC/Project_Pfizer/2022_ZLC_Data/Date_Analysis.csv')
'''




#Inventory Analysis
#Deriving the process order status on a daily basis with the magnitude of change from the previous day
Unique_Order = change_counts.iloc[:,[0,6]]
Unique_Order = pd.merge(Unique_Order,df[['process_order','acquisition_ts_est','process_order_item_plan_qty','diff']], on = 'process_order', how = 'inner')
Unique_Order['Day'] = Unique_Order['acquisition_ts_est'].dt.date
Unique_Order['diff'] = Unique_Order['diff'].abs()
Unique_Order['diff'] = Unique_Order['diff']/24
Unique_Order.rename(columns={"diff":"Diff(D)"},inplace= True)



#summarising the magnitude of change, i.e. grouping by each day for every process order for comparing that magnitude of 
#change with respect to the inventory level on that day 
Change_Vs_Inv = Unique_Order.groupby(['Day','process_order'])['Diff(D)'].sum()
Change_Vs_Inv = Change_Vs_Inv.to_frame()
Change_Vs_Inv.reset_index(inplace = True)
Change_Vs_Inv = pd.merge(Change_Vs_Inv,Unique_Order[['material_key','process_order','Day','process_order_item_plan_qty']], on = ['process_order','Day' ],how = 'left')
Change_Vs_Inv = Change_Vs_Inv.drop_duplicates()
Change_Vs_Inv = Change_Vs_Inv.sort_values(['process_order','Day'], ascending = True)



#Import BOM details and Inventory levels
Bom = pd.read_csv(r'/Users/spandanrout/Desktop/ZLC/Project_Pfizer/2022_ZLC_Data/20220117_BOM.csv')
Inv = pd.read_csv(r'/Users/spandanrout/Desktop/ZLC/Project_Pfizer/2022_ZLC_Data/20220118_Inventory.csv')
Inv.rename(columns={'material_key':'bom_component_key','calendar_day':'Day'}, inplace = True)
Inv['Day'] = pd.to_datetime(Inv['Day'])



#Mapping the material key to its components and calculating the required qty of components based on the recipe
Change_Vs_Inv = pd.merge(Change_Vs_Inv,Bom[['material_key','bom_component_key','issued_quantity','produced_quantity']], on = 'material_key', how = 'inner')
Change_Vs_Inv["Need_BOM"] = (Change_Vs_Inv['process_order_item_plan_qty']* Change_Vs_Inv['issued_quantity'])/Change_Vs_Inv['produced_quantity']
Change_Vs_Inv['Day'] = pd.to_datetime(Change_Vs_Inv['Day'])
Change_Vs_Inv = pd.merge(Change_Vs_Inv,Inv[['Day','bom_component_key','stock_type','quantity']], on =['bom_component_key', 'Day'], how ='inner')
Change_Vs_Inv = Change_Vs_Inv[Change_Vs_Inv["stock_type"] == 'A']
Change_Vs_Inv['Day'] = Change_Vs_Inv['Day'].dt.date



'''
new2 = Change_Vs_Inv.groupby(['Day', 'bom_component_key'])['Need_BOM'].sum()
new2 = new2.to_frame()
new2.reset_index(inplace = True)
Inv['Day'] = Inv['Day'].dt.date
new2 = pd.merge(new2,Inv[['Day','bom_component_key','stock_type','quantity']],on =['bom_component_key', 'Day'], how ='inner')
new2 = new2[new2["stock_type"] == 'A']
new2 = pd.merge(new2, new1[['bom_component_key', 'Day', 'process_order']], on =['bom_component_key', 'Day'], how = 'left')
new2.to_csv(r'/Users/spandanrout/Desktop/ZLC/Project_Pfizer/2022_ZLC_Data/Inventory.csv')
'''



#finding the qty needed on the scheduled start date and mapping the process orders to its components via material key
last_dates['Day'] = last_dates['scheduled_start_datetime'].dt.date 
qty_on_scheduled_start = pd.merge(last_dates,last_change[['process_order','material_key','process_order_item_plan_qty','Day']], on = ['process_order','Day'], how = 'left')
qty_on_scheduled_start = qty_on_scheduled_start.drop_duplicates()
qty_on_scheduled_start = pd.merge(qty_on_scheduled_start,Change_Vs_Inv[['Day','process_order','material_key','bom_component_key','Need_BOM','quantity']], on = ['Day','process_order','material_key'], how = 'left')
groupby_on_start= qty_on_scheduled_start.groupby(['Day', 'bom_component_key'])['Need_BOM'].sum()
groupby_on_start = groupby_on_start.to_frame()
groupby_on_start.reset_index(inplace = True)



#creating a risk profile matrix of the planner based on 
#inventory status on multiple thresholds of time difference of 0,1,3,7,14 days
Inv_buckets = last_dates
Inv_buckets = Inv_buckets.append([Inv_buckets]*4, ignore_index=True)
Inv_buckets = Inv_buckets.sort_values(by =['process_order'])
list_int = [0,1,3,7,14]
Inv_buckets['Buckets'] = np.tile(list_int, len(Inv_buckets)//len(list_int) + 1)[:len(Inv_buckets)]
Inv_buckets['Bucket_Date'] = Inv_buckets['scheduled_start_datetime'] - pd.to_timedelta(Inv_buckets['Buckets'], unit='d')
Inv_buckets['Bucket_Date'] = Inv_buckets['Bucket_Date'].dt.date 



#Mapping the process orders to its components and corresponding needed quantity
Inv_buckets = pd.merge(Inv_buckets,qty_on_scheduled_start[['process_order','bom_component_key']], on = 'process_order', how = 'left')
Inv_buckets = pd.merge(Inv_buckets, groupby_on_start[['bom_component_key','Day','Need_BOM']], left_on =['bom_component_key','Bucket_Date'], right_on=['bom_component_key','Day'], how = 'left' )
Inv_buckets.drop(columns= 'Day_y', inplace = True)



#Filtering only for stock type A
Inv = Inv[Inv['stock_type']=='A']



#Mapping needed quantity to its available inventory of the components on the specific bucket dates for all orders
Inv_Vs_Need_buckets = pd.merge(Inv_buckets, Inv[['Day','bom_component_key','quantity']], left_on =['bom_component_key','Bucket_Date'], right_on=['bom_component_key','Day'], how = 'left')
Inv_Vs_Need_buckets.drop(columns = ['Day_x','Day'], inplace = True)
Inv_Vs_Need_buckets['Need_BOM']= Inv_Vs_Need_buckets['Need_BOM'].fillna(0)
Inv_Vs_Need_buckets['Diff'] = Inv_Vs_Need_buckets['quantity']-Inv_Vs_Need_buckets['Need_BOM']
#Inv_Vs_Need_buckets.to_csv(r'/Users/spandanrout/Desktop/ZLC/Project_Pfizer/2022_ZLC_Data/Buckets.csv')
Final_Risk_Profile = Inv_Vs_Need_buckets.groupby(['bom_component_key','Buckets'])['Diff'].mean()
#Final_Risk_Profile.to_csv(r'/Users/spandanrout/Desktop/ZLC/Project_Pfizer/2022_ZLC_Data/CompBuckets.csv')




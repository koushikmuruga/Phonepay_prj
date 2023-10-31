import os
import json
import mysql.connector
import pandas as pd
import streamlit as st
import requests
import plotly.express as px
import plotly.graph_objects as go

#top_user

path='C:/Users/koush/Downloads/pulse-master/pulse-master/data/top/user/country/india/state'
state_list=os.listdir(path)
columns={'state':[],'year':[],'quarter':[],'pincode':[],'registeredusers':[]}
for state in state_list:
    p1=path+'/'+state
    year_list=os.listdir(p1)

    for year in year_list:
        p2=p1+'/'+year
        file_list=os.listdir(p2)

        for file in file_list:
            json_data=json.load(open(p2+'/'+file,'r'))

            for val in json_data['data']['pincodes']:
                columns['pincode'].append(val['registeredUsers'])
                columns['registeredusers'].append(val['name'])
                columns['state'].append(state)
                columns['year'].append(year)
                columns['quarter'].append(file.strip('.json'))
top_user_df=pd.DataFrame(columns)

top_user_df["state"] = top_user_df["state"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_user_df["state"] = top_user_df["state"].str.replace("-"," ")
top_user_df["state"] = top_user_df["state"].str.title()
top_user_df['state'] = top_user_df['state'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")


#top_transaction

path='C:/Users/koush/Downloads/pulse-master/pulse-master/data/top/transaction/country/india/state'
state_list=os.listdir(path)
columns={'state':[],'year':[],'quarter':[],'pincode':[],'transaction_count':[], 'transaction_amount':[]}
for state in state_list:
    p1=path+'/'+state
    year_list=os.listdir(p1)

    for year in year_list:
        p2=p1+'/'+year
        file_list=os.listdir(p2)

        for file in file_list:
            json_data=json.load(open(p2+'/'+file,'r'))

            for val in json_data['data']['pincodes']:
                columns['pincode'].append(val['entityName'])
                columns['transaction_count'].append(val['metric']['count'])
                columns['transaction_amount'].append(val['metric']['amount'])
                columns['state'].append(state)
                columns['year'].append(year)
                columns['quarter'].append(file.strip('.json'))
top_transaction_df=pd.DataFrame(columns)

top_transaction_df["state"] = top_transaction_df["state"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_transaction_df["state"] = top_transaction_df["state"].str.replace("-"," ")
top_transaction_df["state"] = top_transaction_df["state"].str.title()
top_transaction_df['state'] = top_transaction_df['state'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
 

#map_user

path='C:/Users/koush/Downloads/pulse-master/pulse-master/data/map/user/hover/country/india/state'
state_list=os.listdir(path)
columns={'state':[],'year':[],'quarter':[],'districts':[],'registereduser':[],'appOpens':[]}
for state in state_list:
    p1=path+'/'+state
    year_list=os.listdir(p1)

    for year in year_list:
        p2=p1+'/'+year
        file_list=os.listdir(p2)

        for file in file_list:
            json_data=json.load(open(p2+'/'+file,'r'))

            for val in json_data['data']['hoverData'].items():  #revisit
                columns['districts'].append(val[0])
                columns['registereduser'].append(val[1]["registeredUsers"])
                columns["appOpens"].append(val[1]["appOpens"])
                columns['state'].append(state)
                columns['year'].append(year)
                columns['quarter'].append(file.strip('.json'))
map_user_df=pd.DataFrame(columns)

map_user_df["state"] = map_user_df["state"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_user_df["state"] = map_user_df["state"].str.replace("-"," ")
map_user_df["state"] = map_user_df["state"].str.title()
map_user_df['state'] = map_user_df['state'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")


#map_transaction

path='C:/Users/koush/Downloads/pulse-master/pulse-master/data/map/transaction/hover/country/india/state'
state_list=os.listdir(path)
columns={'state':[],'year':[],'quarter':[],'district':[],'transactioncount':[],'transactionamount':[]}
for state in state_list:
    p1=path+'/'+state
    year_list=os.listdir(p1)

    for year in year_list:
        p2=p1+'/'+year
        file_list=os.listdir(p2)

        for file in file_list:
            json_data=json.load(open(p2+'/'+file,'r'))

            for val in json_data['data']['hoverDataList']:
                columns['district'].append(val['name'])
                columns['transactioncount'].append(val["metric"][0]["count"])
                columns['transactionamount'].append(val["metric"][0]["amount"])
                columns['state'].append(state)
                columns['year'].append(year)
                columns['quarter'].append(file.strip('.json'))
map_transaction_df=pd.DataFrame(columns)

map_transaction_df["state"] = map_transaction_df["state"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_transaction_df["state"] = map_transaction_df["state"].str.replace("-"," ")
map_transaction_df["state"] = map_transaction_df["state"].str.title()
map_transaction_df['state'] = map_transaction_df['state'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")


#aggr_user

path='C:/Users/koush/Downloads/pulse-master/pulse-master/data/aggregated/user/country/india/state'
state_list=os.listdir(path)
columns={'state':[],'year':[],'quarter':[],'brands':[],'transactioncount':[],'percentage':[]}
for state in state_list:
    p1=path+'/'+state
    year_list=os.listdir(p1)

    for year in year_list:
        p2=p1+'/'+year
        file_list=os.listdir(p2)

        for file in file_list:
            json_data=json.load(open(p2+'/'+file,'r'))

            try:

                for val in json_data['data']['usersByDevice']:
                    columns['brands'].append(val['brand'])
                    columns['transactioncount'].append(val['count'])
                    columns['percentage'].append(val['percentage'])
                    columns['state'].append(state)
                    columns['year'].append(year)
                    columns['quarter'].append(file.strip('.json'))

            except:
                pass

aggr_user_df=pd.DataFrame(columns)

aggr_user_df["state"] = aggr_user_df["state"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggr_user_df["state"] = aggr_user_df["state"].str.replace("-"," ")
aggr_user_df["state"] = aggr_user_df["state"].str.title()
aggr_user_df['state'] = aggr_user_df['state'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")


#aggr_transaction

path='C:/Users/koush/Downloads/pulse-master/pulse-master/data/aggregated/transaction/country/india/state'
state_list=os.listdir(path)
columns={'state':[],'year':[],'quarter':[],'transactiontype':[],'transactioncount':[],'transactionamount':[]}
for state in state_list:
    p1=path+'/'+state
    year_list=os.listdir(p1)

    for year in year_list:
        p2=p1+'/'+year
        file_list=os.listdir(p2)

        for file in file_list:
            json_data=json.load(open(p2+'/'+file,'r'))

            for val in json_data['data']['transactionData']:
                columns['transactiontype'].append(val['name'])
                columns['transactioncount'].append(val["paymentInstruments"][0]["count"])
                columns['transactionamount'].append(val["paymentInstruments"][0]["amount"])
                columns['state'].append(state)
                columns['year'].append(year)
                columns['quarter'].append(file.strip('.json'))
aggr_transaction_df=pd.DataFrame(columns)

aggr_transaction_df["state"] = aggr_transaction_df["state"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggr_transaction_df["state"] = aggr_transaction_df["state"].str.replace("-"," ")
aggr_transaction_df["state"] = aggr_transaction_df["state"].str.title()
aggr_transaction_df['state'] = aggr_transaction_df['state'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")


#sql

con=mysql.connector.connect(host='localhost',user='root',password='Mysql@123',database='mdb2')
cur=con.cursor()

#aggr_transaction_table
query_create = '''CREATE TABLE if not exists aggr_transaction (States varchar(50),
                                                                    Years int,
                                                                    Quarter int,
                                                                    Transaction_type varchar(50),
                                                                    Transaction_count bigint,
                                                                    Transaction_amount bigint
                                                                    )'''
cur.execute(query_create)
con.commit()

for index,row in aggr_transaction_df.iterrows():
    query_insert = '''INSERT INTO aggr_transaction (States, Years, Quarter, Transaction_type, Transaction_count, Transaction_amount)
                                                    values(%s,%s,%s,%s,%s,%s)'''
    values = (row["state"],
            row["year"],
            row["quarter"],
            row["transactiontype"],
            row["transactioncount"],
            row["transactionamount"]
            )
    cur.execute(query_insert,values)
    con.commit()

#aggr_user_table
query_create = '''CREATE TABLE if not exists aggr_user (States varchar(50),
                                                            Years int,
                                                            Quarter int,
                                                            Brands varchar(50),
                                                            Transaction_count bigint,
                                                            Percentage float)'''
cur.execute(query_create)
con.commit()

for index,row in aggr_user_df.iterrows():
    query_insert = '''INSERT INTO aggr_user (States, Years, Quarter, Brands, Transaction_count, Percentage)
                                                    values(%s,%s,%s,%s,%s,%s)'''
    values = (row["state"],
            row["year"],
            row["quarter"],
            row["brands"],
            row["transactioncount"],
            row["percentage"])
    cur.execute(query_insert,values)
    con.commit()

#map_transaction_table
query_create = '''CREATE TABLE if not exists map_transaction (States varchar(50),
                                                            Years int,
                                                            Quarter int,
                                                            District varchar(50),
                                                            Transaction_count bigint,
                                                            Transaction_amount float)'''
cur.execute(query_create)
con.commit()

for index,row in map_transaction_df.iterrows():
    query_insert = '''INSERT INTO map_transaction (States, Years, Quarter, District, Transaction_count, Transaction_amount)
                                                    VALUES (%s, %s, %s, %s, %s, %s)'''
    values = (row['state'],
            row['year'],
            row['quarter'],
            row['district'],
            row['transactioncount'],
            row['transactionamount'])
    cur.execute(query_insert,values)
    con.commit() 


#map_user_table
query_create = '''CREATE TABLE if not exists map_user (States varchar(50),
                                                    Years int,
                                                    Quarter int,
                                                    Districts varchar(50),
                                                    RegisteredUser bigint,
                                                    AppOpens bigint)'''
cur.execute(query_create)
con.commit()

for index,row in map_user_df.iterrows():
    query_insert = '''INSERT INTO map_user (States, Years, Quarter, Districts, RegisteredUser, AppOpens)
                    values(%s,%s,%s,%s,%s,%s)'''
    values = (row["state"],
            row["year"],
            row["quarter"],
            row["districts"],
            row["registereduser"],
            row["appOpens"])
    cur.execute(query_insert,values)
    con.commit()

#top_transaction_table
query_create = '''CREATE TABLE if not exists top_transaction (States varchar(50),
                                                            Years int,
                                                            Quarter int,
                                                            pincodes int,
                                                            Transaction_count bigint,
                                                            Transaction_amount bigint)'''
cur.execute(query_create)
con.commit()

for index,row in top_transaction_df.iterrows():
    query_insert = '''INSERT INTO top_transaction (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
                                                values(%s,%s,%s,%s,%s,%s)'''
    values = (row["state"],
            row["year"],
            row["quarter"],
            row["pincode"],
            row["transaction_count"],
            row["transaction_amount"])
    cur.execute(query_insert,values)
    con.commit()

#top_user_table
query_create = '''CREATE TABLE if not exists top_user (States varchar(50),
                                                    Years int,
                                                    Quarter int,
                                                    Pincodes int,
                                                    RegisteredUser bigint
                                                    )'''
cur.execute(query_create)
con.commit()

for index,row in top_user_df.iterrows():
    query_insert = '''INSERT INTO top_user (States, Years, Quarter, Pincodes, RegisteredUser)
                                        values(%s,%s,%s,%s,%s)'''
    values = (row["state"],
            row["year"],
            row["quarter"],
            row["pincode"],
            row["registeredusers"])
    cur.execute(query_insert,values)
    con.commit()


#############

#CREATION OF DATAFRAMES FROM SQL

con1 = mysql.connector.connect(host='localhost',user='root',password='Mysql@123',database='mdb2')
cur = con1.cursor(buffered=True, dictionary=True)

#aggr_transaction
cur.execute("select * from aggr_transaction;")
con1.commit()
tb1 = cur.fetchall()
Aggre_trans = pd.DataFrame(tb1,columns = ("States", "Years", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount"))

#aggr_user
cur.execute("select * from aggr_user")
con1.commit()
tb2 = cur.fetchall()
Aggre_user = pd.DataFrame(tb2,columns = ("States", "Years", "Quarter", "Brands", "Transaction_count", "Percentage"))

#Map_transaction
cur.execute("select * from map_transaction")
con1.commit()
tb3 = cur.fetchall()
Map_trans = pd.DataFrame(tb3,columns = ("States", "Years", "Quarter", "Districts", "Transaction_count", "Transaction_amount"))

#Map_user
cur.execute("select * from map_user")
con1.commit()
tb4 = cur.fetchall()
Map_user = pd.DataFrame(tb4,columns = ("States", "Years", "Quarter", "Districts", "RegisteredUser", "AppOpens"))

#Top_transaction
cur.execute("select * from top_transaction")
con1.commit()
tb5 = cur.fetchall()
Top_trans = pd.DataFrame(tb5,columns = ("States", "Years", "Quarter", "Pincodes", "Transaction_count", "Transaction_amount"))

#Top_user
cur.execute("select * from top_user")
con1.commit()
tb6 = cur.fetchall()
Top_user = pd.DataFrame(tb6, columns = ("States", "Years", "Quarter", "Pincodes", "RegisteredUser"))


def get_all_states():
    url='https://gist.githubusercontent.com/shubhamjain/35ed77154f577295707a/raw/7bc2a915cff003fb1f8ff49c6890576eee4f2f10/IndianStates.json'
    res=requests.get(url)
    data=json.loads(res.content)
    all_state=[]
    for i,j in data.items():
        all_state.append(j)
    return all_state,data

all_state,data1=get_all_states()

def all_amount(all_state):
    
    df_state_names_tra = pd.DataFrame({"States":all_state})

    frames = []

    for year in Map_user["Years"].unique():
        for quarter in Aggre_trans["Quarter"].unique():

            at1 = Aggre_trans[(Aggre_trans["Years"]==year)&(Aggre_trans["Quarter"]==quarter)]
            atf1 = at1[["States","Transaction_amount"]]
            atf1 = atf1.sort_values(by="States")
            atf1["Years"]=year              
            atf1["Quarter"]=quarter         
            frames.append(atf1)

    merged_df = pd.concat(frames)           

    fig_tra = px.choropleth(merged_df, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_amount",
                            color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                            animation_frame="Years", animation_group="Quarter")

    fig_tra.update_geos(fitbounds= "locations", visible =False)
    fig_tra.update_layout(width =600, height= 700)
    fig_tra.update_layout(title_font= {"size":25})
    return st.plotly_chart(fig_tra)

def payment_count():
    attype= Aggre_trans[["Transaction_type", "Transaction_count"]]
    att1= attype.groupby("Transaction_type")["Transaction_count"].sum()
    df_att1= pd.DataFrame(att1).reset_index()
    fig_pc= px.bar(df_att1,x= "Transaction_type",y= "Transaction_count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                color_discrete_sequence=px.colors.sequential.Redor_r)
    fig_pc.update_layout(width=600, height= 500)
    return st.plotly_chart(fig_pc)

def all_count(all_state):
    
    df_state_names_tra= pd.DataFrame({"States":all_state})

    frames= []

    for year in Aggre_trans["Years"].unique():
        for quarter in Aggre_trans["Quarter"].unique():

            at1= Aggre_trans[(Aggre_trans["Years"]==year)&(Aggre_trans["Quarter"]==quarter)]
            atf1= at1[["States", "Transaction_count"]]
            atf1=atf1.sort_values(by="States")
            atf1["Years"]=year
            atf1["Quarter"]=quarter
            frames.append(atf1)

    merged_df = pd.concat(frames)

    fig_tra= px.choropleth(merged_df, geojson= data1, locations= "States",featureidkey= "properties.ST_NM",
                        color= "Transaction_count", color_continuous_scale="Sunsetdark", range_color= (0,3000000),
                        title="TRANSACTION COUNT", hover_name= "States", animation_frame= "Years", animation_group= "Quarter")

    fig_tra.update_geos(fitbounds= "locations", visible= False)
    fig_tra.update_layout(width= 600, height= 700)
    fig_tra.update_layout(title_font={"size":25})
    return st.plotly_chart(fig_tra)

def payment_amount():
    attype= Aggre_trans[["Transaction_type","Transaction_amount"]]
    att1= attype.groupby("Transaction_type")["Transaction_amount"].sum()
    df_att1= pd.DataFrame(att1).reset_index()
    fig_tra_pa= px.bar(df_att1, x= "Transaction_type", y= "Transaction_amount", title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                    color_discrete_sequence= px.colors.sequential.Blues_r)
    fig_tra_pa.update_layout(width= 600, height= 500)
    return st.plotly_chart(fig_tra_pa)

def reg_all_states(state):
    mu= Map_user[["States","Districts","RegisteredUser"]]
    mu1= mu.loc[(mu["States"]==state)]
    mu2= mu1[["Districts", "RegisteredUser"]]
    mu3= mu2.groupby("Districts")["RegisteredUser"].sum()
    mu4= pd.DataFrame(mu3).reset_index()
    fig_mu= px.bar(mu4, x= "Districts", y= "RegisteredUser", title= "DISTRICTS and REGISTERED USER",
                color_discrete_sequence=px.colors.sequential.Bluered_r)
    fig_mu.update_layout(width= 1000, height= 500)
    return st.plotly_chart(fig_mu)

def transaction_amount_year(sel_year,all_state):
    
    year= int(sel_year)
    atay= Aggre_trans[["States","Years","Transaction_amount"]]
    atay1= atay.loc[(Aggre_trans["Years"]==year)]
    atay2= atay1.groupby("States")["Transaction_amount"].sum()
    atay3= pd.DataFrame(atay2).reset_index()

    fig_atay= px.choropleth(atay3, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                            color= "Transaction_amount", color_continuous_scale="rainbow", range_color=(0,800000000000),
                            title="TRANSACTION AMOUNT and STATES", hover_name= "States")

    fig_atay.update_geos(fitbounds= "locations", visible= False)
    fig_atay.update_layout(width=600,height=700)
    fig_atay.update_layout(title_font= {"size":25})
    return st.plotly_chart(fig_atay)

def payment_count_year(sel_year):
    year= int(sel_year)
    apc= Aggre_trans[["Transaction_type", "Years", "Transaction_count"]]
    apc1= apc.loc[(Aggre_trans["Years"]==year)]
    apc2= apc1.groupby("Transaction_type")["Transaction_count"].sum()
    apc3= pd.DataFrame(apc2).reset_index()

    fig_apc= px.bar(apc3,x= "Transaction_type", y= "Transaction_count", title= "PAYMENT COUNT and PAYMENT TYPE",
                    color_discrete_sequence=px.colors.sequential.Brwnyl_r)
    fig_apc.update_layout(width=600, height=500)
    return st.plotly_chart(fig_apc)


def transaction_count_year(sel_year):
    
    year= int(sel_year)
    atcy= Aggre_trans[["States", "Years", "Transaction_count"]]
    atcy1= atcy.loc[(Aggre_trans["Years"]==year)]
    atcy2= atcy1.groupby("States")["Transaction_count"].sum()
    atcy3= pd.DataFrame(atcy2).reset_index()

    fig_atcy= px.choropleth(atcy3, geojson=data1, locations= "States", featureidkey= "properties.ST_NM",
                            color= "Transaction_count", color_continuous_scale= "rainbow",range_color=(0,3000000000),
                            title= "TRANSACTION COUNT and STATES",hover_name= "States")
    fig_atcy.update_geos(fitbounds= "locations", visible= False)
    fig_atcy.update_layout(width=600, height= 700)
    fig_atcy.update_layout(title_font={"size":25})
    return st.plotly_chart(fig_atcy)


def payment_amount_year(sel_year):
    year= int(sel_year)
    apay = Aggre_trans[["Years", "Transaction_type", "Transaction_amount"]]
    apay1= apay.loc[(Aggre_trans["Years"]==year)]
    apay2= apay1.groupby("Transaction_type")["Transaction_amount"].sum()
    apay3= pd.DataFrame(apay2).reset_index()

    fig_apay= px.bar(apay3, x="Transaction_type", y= "Transaction_amount", title= "PAYMENT TYPE and PAYMENT AMOUNT",
                    color_discrete_sequence=px.colors.sequential.Burg_r)
    fig_apay.update_layout(width=600, height=500)
    return st.plotly_chart(fig_apay)


def reg_state_all_transamt(sel_year,state):
    year= int(sel_year)
    mts= Map_trans[["States", "Years","Districts", "Transaction_amount"]]
    mts1= mts.loc[(Map_trans["States"]==state)&(Map_trans["Years"]==year)]
    mts2= mts1.groupby("Districts")["Transaction_amount"].sum()
    mts3= pd.DataFrame(mts2).reset_index()

    fig_mts= px.bar(mts3, x= "Districts", y= "Transaction_amount", title= "DISTRICT and TRANSACTION AMOUNT",
                    color_discrete_sequence= px.colors.sequential.Darkmint_r)
    fig_mts.update_layout(width= 600, height= 500)
    return st.plotly_chart(fig_mts)


def q1():
    lt= Aggre_trans[["States", "Transaction_amount"]]
    lt1= lt.groupby("States")["Transaction_amount"].sum().sort_values(ascending= True)
    lt2= pd.DataFrame(lt1).reset_index().head(10)

    fig_lts= px.bar(lt2, x= "States", y= "Transaction_amount",title= "LOWEST TRANSACTION AMOUNT and STATES",
                    color_discrete_sequence= px.colors.sequential.Oranges_r)
    return st.plotly_chart(fig_lts)

def q2():
    ht= Aggre_trans[["States", "Transaction_amount"]]
    ht1= ht.groupby("States")["Transaction_amount"].sum().sort_values(ascending= False)
    ht2= pd.DataFrame(ht1).reset_index().head(10)

    fig_lts= px.bar(ht2, x= "States", y= "Transaction_amount",title= "HIGHEST TRANSACTION AMOUNT and STATES",
                    color_discrete_sequence= px.colors.sequential.Oranges_r)
    return st.plotly_chart(fig_lts)

def q3():
    htd= Map_trans[["Districts", "Transaction_amount"]]
    htd1= htd.groupby("Districts")["Transaction_amount"].sum().sort_values(ascending=False)
    htd2= pd.DataFrame(htd1).reset_index()

    fig_htd= px.pie(htd2, values= "Transaction_amount", names= "Districts", title="DISTRICTS WITH HIGHEST TRANSACTION AMOUNT",
                    color_discrete_sequence=px.colors.sequential.Emrld_r)
    return st.plotly_chart(fig_htd)

def q4():
    htd= Map_trans[["Districts", "Transaction_amount"]]
    htd1= htd.groupby("Districts")["Transaction_amount"].sum().sort_values()
    htd2= pd.DataFrame(htd1).reset_index()

    fig_htd= px.pie(htd2, values= "Transaction_amount", names= "Districts", title="DISTRICTS WITH LOWEST TRANSACTION AMOUNT",
                    color_discrete_sequence=px.colors.sequential.Emrld_r)
    return st.plotly_chart(fig_htd)

def q5():
    htd= Map_trans[["Districts", "Transaction_amount"]]
    htd1= htd.groupby("Districts")["Transaction_amount"].sum().sort_values(ascending=False)
    htd2= pd.DataFrame(htd1).head(10).reset_index()

    fig_htd= px.pie(htd2, values= "Transaction_amount", names= "Districts", title="TOP 10 DISTRICTS OF HIGHEST TRANSACTION AMOUNT",
                    color_discrete_sequence=px.colors.sequential.Greens_r)
    return st.plotly_chart(fig_htd)

def q6():
    stc= Aggre_trans[["States", "Transaction_count"]]
    stc1= stc.groupby("States")["Transaction_count"].sum().sort_values(ascending=True)
    stc2= pd.DataFrame(stc1).reset_index()

    fig_stc= px.bar(stc2, x= "States", y= "Transaction_count", title= "STATES WITH LOWEST TRANSACTION COUNT",
                    color_discrete_sequence= px.colors.sequential.Jet_r)
    return st.plotly_chart(fig_stc)

def q7():
    stc= Aggre_trans[["States", "Transaction_count"]]
    stc1= stc.groupby("States")["Transaction_count"].sum().sort_values(ascending=False)
    stc2= pd.DataFrame(stc1).reset_index()

    fig_stc= px.bar(stc2, x= "States", y= "Transaction_count", title= "STATES WITH HIGHEST TRANSACTION COUNT",
                    color_discrete_sequence= px.colors.sequential.Magenta_r)
    return st.plotly_chart(fig_stc)

#streamlit

st.set_page_config(layout= "wide")

st.title("PHONEPE DATA VISUALIZATION AND EXPLORATION")
tab1, tab2 = st.tabs(["***DATA***","***CHARTS***"])


with tab1:
    sel_year = st.selectbox("select the Year",("All", "2018", "2019", "2020", "2021", "2022", "2023"))
    if sel_year == "All" :
        col1, col2 = st.columns(2)
        with col1:
            all_amount(all_state)
            payment_count()
            
        with col2:
            all_count(all_state)
            payment_amount()

        state=st.selectbox("selecet the state",('Andaman & Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh',
                                                'Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh',
                                                'Dadra and Nagar Haveli and Daman and Diu', 'Delhi', 'Goa',
                                                'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir',
                                                'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep',
                                                'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram',
                                                'Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan',
                                                'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh',
                                                'Uttarakhand', 'West Bengal'))
        reg_all_states(state)

    else:
        col1,col2= st.columns(2)

        with col1:
            transaction_amount_year(sel_year,all_state)
            payment_count_year(sel_year)

        with col2:
            transaction_count_year(sel_year)
            payment_amount_year(sel_year)
            state= st.selectbox("selecet the state",('Andaman & Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh',
                                                'Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh',
                                                'Dadra and Nagar Haveli and Daman and Diu', 'Delhi', 'Goa',
                                                'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir',
                                                'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep',
                                                'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram',
                                                'Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan',
                                                'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh',
                                                'Uttarakhand', 'West Bengal'))
            
            reg_state_all_transamt(sel_year,state)

with tab2:
    ques= st.selectbox("select the question",('States With Lowest Trasaction Amount',
                                  'States With Highest Trasaction Amount','Districts With Highest Transaction Amount',
                                  'Districts With Lowest Transaction Amount','Top 10 Districts With Highest Transaction Amount','States With Lowest Trasaction Count',
                                  'States With Highest Trasaction Count'))
    
    if ques=="States With Lowest Trasaction Amount":
        q1()

    elif ques=="States With Highest Trasaction Amount":
        q2()

    elif ques=="Districts With Highest Transaction Amount":
        q3()

    elif ques=="Districts With Lowest Transaction Amount":
        q4()

    elif ques=="Top 10 Districts With Highest Transaction Amount":
        q5()
    
    elif ques=="States With Lowest Trasaction Count":
        q6()

    elif ques=="States With Highest Trasaction Count":
        q7()
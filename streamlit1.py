import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import mysql.connector
import requests
import json
import plotly_express as px 
import numpy as np
from PIL import Image

# Connect to MySQL database
try:
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="phonepe"
    )
    cursor = connection.cursor()
except mysql.connector.Error as e:
    st.error(f"Error connecting to MySQL: {e}")
    st.stop()

# If connection successful, proceed with Streamlit app
im = Image.open("G:\phonepe\png-transparent-phonepe-india-unified-payments-interface-india-purple-violet-text.png")
st.set_page_config(
    page_title="PHONEPE PULSE DATA VISUALIZATION",
    page_icon=im,
    layout="wide",
    )
# Add note
st.markdown("(Note: This data is between 2018 to 2023 in INDIA)")

# Selection option using dropdown
option = st.selectbox('Select your option', ('All India', 'State wise', 'Top Ten categories'), index=0)

if option =='All India':
# Function to fetch transaction data
  def fetch_transaction_data(trans_yr, trans_qtr, trans_type):
    try:
        # Transaction Analysis bar chart query
        cursor.execute(f"SELECT State, Trans_amount FROM aggregated_transaction WHERE Year = '{trans_yr}' AND Quarter = '{trans_qtr}' AND Trans_type = '{trans_type}';")
        trans_qry_rslt = cursor.fetchall()
        df_trans__qry_rslt = pd.DataFrame(trans_qry_rslt, columns=['State', 'Trans_amount'])

        # Transaction Analysis table query
        cursor.execute(f"SELECT State, Trans_count, Trans_amount FROM aggregated_transaction WHERE Year = '{trans_yr}' AND Quarter = '{trans_qtr}' AND Trans_type = '{trans_type}';")
        trans_anly_tab_qry = cursor.fetchall()
        df_trans_anly_tab_qry = pd.DataFrame(trans_anly_tab_qry, columns=['State','Trans_count','Trans_amount'])

        # Total Transaction Amount table query
        cursor.execute(f"SELECT SUM(Trans_amount), AVG(Trans_amount) FROM aggregated_transaction WHERE Year = '{trans_yr}' AND Quarter = '{trans_qtr}' AND Trans_type = '{trans_type}';")
        trans_amt_qry= cursor.fetchall()
        df_trans_amt_qry = pd.DataFrame(trans_amt_qry, columns=['Total','Average'])

        # Total Transaction Count table query
        cursor.execute(f"SELECT SUM(Trans_count), AVG(Trans_count) FROM aggregated_transaction WHERE Year = '{trans_yr}' AND Quarter = '{trans_qtr}' AND Trans_type = '{trans_type}';")
        trans_count_qry = cursor.fetchall()
        df_trans_count_qry = pd.DataFrame(trans_count_qry, columns=['Total','Average'])

        return df_trans__qry_rslt, df_trans_anly_tab_qry, df_trans_amt_qry, df_trans_count_qry

    except mysql.connector.Error as e:
        st.error(f"Error executing MySQL query: {e}")
        st.stop()

# Function to fetch user data
  def fetch_user_data(user_year, user_qtr):
    try:
        # User Analysis Bar chart query
        cursor.execute(f"SELECT State, SUM(User_Count) FROM aggregated_user WHERE Year = '{user_year}' AND Quarter = '{user_qtr}' GROUP BY State;")
        user_tab_qry_rslt = cursor.fetchall()
        df_user_tab_qry = pd.DataFrame(user_tab_qry_rslt, columns=['State', 'User_Count'])
        
        # Transaction Analysis table query
        cursor.execute(f"SELECT State, SUM(User_Count) FROM aggregated_user WHERE Year = '{user_year}' AND Quarter = '{user_qtr}'GROUP BY State ;")
        user_anly_tab_qry = cursor.fetchall()
        df_user_anly_tab_qry = pd.DataFrame(user_anly_tab_qry, columns=['State','User_Count',])

        # Total User Count table query
        cursor.execute(f"SELECT SUM(User_Count), AVG(User_Count) FROM aggregated_user WHERE Year = '{user_year}' AND Quarter = '{user_qtr}';")
        user_count_qry = cursor.fetchall()
        df_user_count_qry = pd.DataFrame(user_count_qry, columns=['Total','Average'])

        return  df_user_anly_tab_qry,df_user_tab_qry, df_user_count_qry

    except mysql.connector.Error as e:
        st.error(f"Error executing MySQL query: {e}")
        st.stop()

# Main code
  option = st.radio("Select Analysis", ("Transaction", "User"))

  if option == "Transaction":
    st.subheader("Transaction Analysis")
    trans_yr = st.selectbox('Select Year', ('2018','2019','2020','2021','2022','2023'))
    trans_qtr = st.selectbox('Select Quarter', ('1','2','3','4'))
    trans_type = st.selectbox('Select Transaction type', ('Recharge & bill payments','Peer-to-peer payments',
                                                           'Merchant payments','Financial Services','Others'))

    # Fetch transaction data
    df_trans__qry_rslt, df_trans_anly_tab_qry, df_trans_amt_qry, df_trans_count_qry = fetch_transaction_data(trans_yr, trans_qtr, trans_type)

    # Plot Transaction Analysis bar chart
    fig_bar = go.Figure()
    fig_bar.add_trace(go.Bar(x=df_trans__qry_rslt['State'], y=df_trans__qry_rslt['Trans_amount']))
    fig_bar.update_layout(title='Transaction Analysis Bar Chart', xaxis_title='State', yaxis_title='Transaction Amount')

    # Geo visualization dashboard for Transaction
    # Drop a State column from df_in_tr_tab_qry_rslt
    df_trans__qry_rslt.drop(columns=['State'], inplace=True)
    # Clone the gio data
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data1 = json.loads(response.content)
    # Extract state names and sort them in alphabetical order
    state_names_tra = [feature['properties']['ST_NM'] for feature in data1['features']]
    state_names_tra.sort()
    # Create a DataFrame with the state names column
    df_state_names_tra = pd.DataFrame({'State': state_names_tra})
    # Combine the Gio State name with df_in_tr_tab_qry_rslt
    df_state_names_tra['Transaction_amount'] = df_trans__qry_rslt
    # convert dataframe to csv file
    df_state_names_tra.to_csv('State_trans.csv', index=False)
    # Read csv
    df_tra = pd.read_csv('State_trans.csv')
    # Geo plot
    fig_tra = px.choropleth(
        df_tra,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',locations='State',color='Transaction_amount',color_continuous_scale='thermal',title='Transaction Analysis'
    )
    fig_tra.update_geos(fitbounds="locations", visible=False)
    fig_tra.update_layout(title_font=dict(size=33),title_font_color='blue', height=800)
    st.plotly_chart(fig_tra,use_container_width=True)
      # Display results
    st.plotly_chart(fig_bar)
#Display the table into sidebar
    st.sidebar.header(':violet[Total calculation]')
    st.sidebar.subheader('Transaction Analysis')
    st.sidebar.dataframe(df_trans_anly_tab_qry)
    st.sidebar.subheader('Transaction Amount')
    st.sidebar.dataframe(df_trans_amt_qry)
    st.sidebar.subheader('Transaction Count')
    st.sidebar.dataframe(df_trans_count_qry)


  else:
    st.subheader("User Analysis")
    user_year = st.selectbox('Select Year', ('2018','2019','2020','2021','2022','2023'), key='in_us_yr')
    user_qtr = st.selectbox('Select Quarter', ('1','2','3','4'), key='in_us_qtr')

    # Fetch user data
    df_user_anly_tab_qry,df_user_tab_qry, df_user_count_qry = fetch_user_data(user_year, user_qtr)

 

    fig_pie = px.pie(df_user_tab_qry, values='User_Count', names='State', title='User Analysis Pie Chart')
    fig_pie.update_layout(title_font=dict(size=33), title_font_color='#6739b7')
    st.plotly_chart(fig_pie, use_container_width=True)
  

     # Drop a State column from df_in_tr_tab_qry_rslt
    df_user_tab_qry.drop(columns=['State'], inplace=True)
    # Clone the gio data
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data2 = json.loads(response.content)
    # Extract state names and sort them in alphabetical order
    state_names_user = [feature['properties']['ST_NM'] for feature in data2['features']]
    state_names_user.sort()
    # Create a DataFrame with the state names column
    df_state_names_user = pd.DataFrame({'State': state_names_user})
    # Combine the Gio State name with df_in_tr_tab_qry_rslt
    df_state_names_user['User_Count'] = df_user_tab_qry
    # convert dataframe to csv file
    df_state_names_user.to_csv('State_user.csv', index=False)
    # Read csv
    df_user = pd.read_csv('State_user.csv')
    # Geo plot
    fig_tra = px.choropleth(
        df_user,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',locations='State',color='User_Count',color_continuous_scale='thermal',title='Transaction Analysis'
    )
    fig_tra.update_geos(fitbounds="locations", visible=False)
    fig_tra.update_layout(title_font=dict(size=33),title_font_color='blue', height=800)
    st.plotly_chart(fig_tra,use_container_width=True)
   

    # Display Total calculation
    st.sidebar.header(':violet[Total calculation]')
    st.sidebar.subheader('User Table')
    st.sidebar.dataframe(df_user_anly_tab_qry)
    st.sidebar.subheader('User Analysis')
    st.sidebar.dataframe(df_user_tab_qry)
    st.sidebar.subheader('User Count')
    st.sidebar.dataframe(df_user_count_qry)


########################################################################################
elif option =='State wise':
# Function to fetch transaction data
  def fetch_transaction_data(trans_state, state_trans_yr, state_trans_qtr):
    try:
         # SQL Query

        # Transaction Analysis bar chart query
        cursor.execute(f"SELECT Trans_type, Trans_amount FROM aggregated_transaction WHERE State = '{trans_state}' AND Year = '{state_trans_yr}' AND Quarter = '{state_trans_qtr}';")
        state_trans_bar_qry = cursor.fetchall()
        df_state_trans_bar_qry = pd.DataFrame(np.array(state_trans_bar_qry), columns=['Trans_type', 'Trans_amount'])
        df_state_trans_bar_qry1 = df_state_trans_bar_qry.set_index(pd.Index(range(1, len(df_state_trans_bar_qry)+1)))

        # Transaction Analysis table query
        cursor.execute(f"SELECT Trans_type, Trans_count, Trans_amount FROM aggregated_transaction WHERE State = '{trans_state}' AND Year = '{state_trans_yr}' AND Quarter = '{state_trans_qtr}';")
        state_trans_anly_qry = cursor.fetchall()
        df_state_trans_anly_qry = pd.DataFrame(np.array(state_trans_anly_qry), columns=['Trans_type','Trans_count','Trans_amount'])
        df_state_trans_anly_qry1 = df_state_trans_anly_qry.set_index(pd.Index(range(1, len(df_state_trans_anly_qry)+1)))

        # Total Transaction Amount table query
        cursor.execute(f"SELECT SUM(Trans_amount), AVG(Trans_amount) FROM aggregated_transaction WHERE State = '{trans_state}' AND Year = '{state_trans_yr}' AND Quarter = '{state_trans_qtr}';")
        state_trans_amount_qry = cursor.fetchall()
        df_state_trans_amount_qry = pd.DataFrame(np.array(state_trans_amount_qry), columns=['Total','Average'])
        df_state_trans_amount_qry1 = df_state_trans_amount_qry.set_index(['Average'])
        
        # Total Transaction Count table query
        cursor.execute(f"SELECT SUM(Trans_count), AVG(Trans_count) FROM aggregated_transaction WHERE State = '{trans_state}' AND Year ='{state_trans_yr}' AND Quarter = '{state_trans_qtr}';")
        state_trans_count_qry = cursor.fetchall()
        df_state_trans_count_qry = pd.DataFrame(np.array(state_trans_count_qry), columns=['Total','Average'])
        df_state_trans_count_qry1 = df_state_trans_count_qry.set_index(['Average'])
       
        return df_state_trans_bar_qry1, df_state_trans_anly_qry1, df_state_trans_amount_qry1,df_state_trans_count_qry1

    except mysql.connector.Error as e:
        st.error(f"Error executing MySQL query: {e}")
        st.stop()

# Function to fetch user data
  def fetch_user_data(state_user, state_user_year,state_user_qtr):
    try:
      # SQL Query

        # User Analysis Bar chart query
        cursor.execute(f"SELECT Quarter, SUM(User_Count) FROM aggregated_user WHERE State = '{state_user}' AND Year = '{state_user_year}' GROUP BY Quarter;")
        st_us_tab_qry_rslt = cursor.fetchall()
        df_st_us_tab_qry_rslt = pd.DataFrame(np.array(st_us_tab_qry_rslt), columns=['Quarter', 'User Count'])
        df_st_us_tab_qry_rslt1 = df_st_us_tab_qry_rslt.set_index(pd.Index(range(1, len(df_st_us_tab_qry_rslt)+1)))

        # Total User Count table query
        cursor.execute(f"SELECT SUM(User_Count), AVG(User_Count) FROM aggregated_user WHERE State = '{state_user}' AND Year = '{state_user_year}';")
        st_us_co_qry_rslt = cursor.fetchall()
        df_st_us_co_qry_rslt = pd.DataFrame(np.array(st_us_co_qry_rslt), columns=['Total','Average'])
        df_st_us_co_qry_rslt1 = df_st_us_co_qry_rslt.set_index(['Average'])
        
        return df_st_us_tab_qry_rslt1, df_st_us_co_qry_rslt1,state_user_qtr

    except mysql.connector.Error as e:
        st.error(f"Error executing MySQL query: {e}")
        st.stop()

# Main code
  option = st.radio("Select Analysis", ("Transaction", "User"))

  if option == "Transaction":
    st.subheader("State Transaction Analysis")
    trans_state = st.selectbox('**Select State**',('andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh','assam', 'bihar', 
            'chandigarh', 'chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 
            'jammu-&-kashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh','maharashtra', 'manipur', 
            'meghalaya', 'mizoram', 'nagaland','odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim', 'tamil-nadu', 'telangana', 
            'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal'),key='st_tr_st')
        
    state_trans_yr = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='st_tr_yr')
        
    state_trans_qtr = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='st_tr_qtr')
    
# Fetch transaction data
    df_state_trans_bar_qry1, df_state_trans_anly_qry1, df_state_trans_amount_qry1,df_state_trans_count_qry1 = fetch_transaction_data(trans_state, state_trans_yr, state_trans_qtr)

  # Plot Transaction Analysis bar chart
 # Convert Trans_amount to float
    df_state_trans_bar_qry1['Trans_amount'] = df_state_trans_bar_qry1['Trans_amount'].astype(float)

# Create a Bar chart for Transaction Analysis
    fig_bar_state = go.Figure()

    fig_bar_state.add_trace(go.Bar(
    x=df_state_trans_bar_qry1['Trans_type'],
    y=df_state_trans_bar_qry1['Trans_amount'],
    marker=dict(color=df_state_trans_bar_qry1['Trans_amount'], colorbar=dict(title='Trans Amount')),
    hovertext=df_state_trans_bar_qry1['Trans_amount'],
    hoverinfo='text+y'
))

    fig_bar_state.update_layout(
    title='Transaction Analysis Chart',
    xaxis_title='Transaction Type',
    yaxis_title='Transaction Amount',
    title_font=dict(size=33),
    title_font_color='#6739b7',
    height=500
)
#Display BAR CHART
    st.plotly_chart(fig_bar_state, use_container_width=True)

#Display the table into sidebar
    st.sidebar.subheader('Transaction Analysis')
    st.sidebar.dataframe(df_state_trans_anly_qry1)
    st.sidebar.subheader('Transaction Amount')
    st.sidebar.dataframe(df_state_trans_amount_qry1)
    st.sidebar.subheader('Transaction Count')
    st.sidebar.dataframe(df_state_trans_count_qry1)
   
  else:
    st.subheader("State User Analysis")
    state_user = st.selectbox('**Select State**',('andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh','assam', 'bihar', 
            'chandigarh', 'chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 
            'jammu-&-kashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh','maharashtra', 'manipur', 
            'meghalaya', 'mizoram', 'nagaland','odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim', 'tamil-nadu', 'telangana', 
            'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal'),key='st_us_st')
        
    state_user_year = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='st_us_yr')
    state_user_qtr = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='state_user_qtr')

# Fetch user data
    df_st_us_tab_qry_rslt1, df_st_us_co_qry_rslt1,state_user_qtr= fetch_user_data(state_user, state_user_year,state_user_qtr)

# Display pie chart for User Analysis
    fig = go.Figure()

    fig.add_trace(go.Pie(
    labels=df_st_us_tab_qry_rslt1['Quarter'],
    values=df_st_us_tab_qry_rslt1['User Count'],
    hole=0.3
))

    fig.update_layout(
    title='User Analysis Pie Chart',
    title_font=dict(size=33),
    title_font_color='#6739b7',
    height=500
)
    st.plotly_chart(fig, use_container_width=True)

# Display Total calculation
    
     
    st.sidebar.subheader('User Analysis')
    st.sidebar.dataframe(df_st_us_tab_qry_rslt1)
    st.sidebar.subheader('User Count')
    st.sidebar.dataframe(df_st_us_co_qry_rslt1)

else:
# Function to fetch transaction data
  def fetch_toptencat_trans_data(top_tr_yr):
    try:
         # SQL Query

        # Top Transaction Analysis bar chart query
        cursor.execute(f"SELECT State, SUM(Trans_amount) As Trans_amount FROM top_transaction WHERE Year = '{top_tr_yr}' GROUP BY State ORDER BY Trans_amount DESC LIMIT 10;")
        top_tr_tab_qry_rslt = cursor.fetchall()
        df_top_tr_tab_qry_rslt = pd.DataFrame(np.array(top_tr_tab_qry_rslt), columns=['State', 'Top Transaction amount'])
        df_top_tr_tab_qry_rslt1 = df_top_tr_tab_qry_rslt.set_index(pd.Index(range(1, len(df_top_tr_tab_qry_rslt)+1)))

        # Top Transaction Analysis table query
        cursor.execute(f"SELECT State, SUM(Trans_amount) as Trans_amount, SUM(Trans_count) as Trans_count FROM top_transaction WHERE Year = '{top_tr_yr}' GROUP BY State ORDER BY Trans_amount DESC LIMIT 10;")
        top_tr_anly_tab_qry_rslt = cursor.fetchall()
        df_top_tr_anly_tab_qry_rslt = pd.DataFrame(np.array(top_tr_anly_tab_qry_rslt), columns=['State', 'Top Transaction amount','Total Transaction count'])
        df_top_tr_anly_tab_qry_rslt1 = df_top_tr_anly_tab_qry_rslt.set_index(pd.Index(range(1, len(df_top_tr_anly_tab_qry_rslt)+1)))

        return df_top_tr_tab_qry_rslt1, df_top_tr_anly_tab_qry_rslt1

    except mysql.connector.Error as e:
        st.error(f"Error executing MySQL query: {e}")
        st.stop()

# Function to fetch user data
  def fetch_toptencat_user_data(top_us_yr):
    try:
       # SQL Query

        # Top User Analysis bar chart query
        cursor.execute(f"SELECT State, SUM(Registered_User) AS Top_user FROM top_user WHERE Year='{top_us_yr}' GROUP BY State ORDER BY Top_user DESC LIMIT 10;")
        top_us_tab_qry_rslt = cursor.fetchall()
        df_top_us_tab_qry_rslt = pd.DataFrame(np.array(top_us_tab_qry_rslt), columns=['State', 'Total User count'])
        df_top_us_tab_qry_rslt1 = df_top_us_tab_qry_rslt.set_index(pd.Index(range(1, len(df_top_us_tab_qry_rslt)+1)))

        return df_top_us_tab_qry_rslt1

    except mysql.connector.Error as e:
        st.error(f"Error executing MySQL query: {e}")
        st.stop()

# Main code
  option = st.radio("Select Analysis", ("Transaction", "User"))
  if option == "Transaction":
    st.subheader("Top Tencat Transaction Analysis")
    top_tr_yr = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='top_tr_yr')

    # Fetch transaction data
    df_top_tr_tab_qry_rslt, df_top_tr_anly_tab_qry_rslt1 = fetch_toptencat_trans_data(top_tr_yr)

    df_top_tr_tab_qry_rslt['State'] = df_top_tr_tab_qry_rslt['State'].astype(str)
    df_top_tr_tab_qry_rslt['Top Transaction amount'] = df_top_tr_tab_qry_rslt['Top Transaction amount'].astype(float) 
 
 # Create bar chart
    df_top_tr_tab_qry_rslt1_fig = px.bar(
    df_top_tr_tab_qry_rslt,
    x='State',
    y='Top Transaction amount',
    color='Top Transaction amount',
    color_continuous_scale='thermal',
    title='Top Transaction Analysis Chart',
    height=600
)

# Update layout
    df_top_tr_tab_qry_rslt1_fig.update_layout(
    title_font=dict(size=33),
    title_font_color='#6739b7'
)

# Show plot
    st.plotly_chart(df_top_tr_tab_qry_rslt1_fig, use_container_width=True)
                      
#Display the table     
    st.sidebar.header('Total calculation')
    st.sidebar.subheader('Top Transaction Analysis')
    st.sidebar.dataframe(df_top_tr_anly_tab_qry_rslt1)
    
  else:
    st.subheader("Top tencat User Analysis")
    top_us_yr = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='top_us_yr')

    # Fetch user data
    df_top_us_tab_qry_rslt1= fetch_toptencat_user_data(top_us_yr)

    # Display pie chart for User Analysis
    df_top_us_tab_qry_rslt1['State'] = df_top_us_tab_qry_rslt1['State'].astype(str)
    df_top_us_tab_qry_rslt1['Total User count'] = df_top_us_tab_qry_rslt1['Total User count'].astype(float)

# Create pie chart
    df_top_us_tab_qry_rslt1_fig = px.pie(
    df_top_us_tab_qry_rslt1,
    values='Total User count',
    names='State',
    title='Top User Analysis Pie Chart'
)

# Update layout
    df_top_us_tab_qry_rslt1_fig.update_layout(
    title_font=dict(size=33),
    title_font_color='#6739b7'
)

# Show plot
    st.plotly_chart(df_top_us_tab_qry_rslt1_fig, use_container_width=True)

# Display Total calculation
    st.sidebar.header('Total calculation')
    st.sidebar.subheader('Total User Analysis')
    st.sidebar.dataframe(df_top_us_tab_qry_rslt1)


from re import A
from typing import AnyStr
from pyspark.sql import SparkSession, dataframe
spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()
import streamlit as st
import pandas as pd
import numpy as np
import plotly.figure_factory as ff
import matplotlib.pyplot as plt
import plotly.express as px
def load_data():
    from pyspark.sql.types import StructField,IntegerType, StructType,StringType, TimestampType
    newDF=[StructField('video_id',StringType(),True),
       StructField('trending_date',StringType(),True),
       StructField('title',StringType(),True),
       StructField('channel_title',StringType(),True),
       StructField('category_id',IntegerType(),True),
       StructField('publish_time',TimestampType(),False),
       StructField('tags',StringType(),False),
       StructField('views',IntegerType(),True),
       StructField('likes',IntegerType(),True),
       StructField('dislikes',StringType(),True),
       StructField('comment_count',IntegerType(),True),
       StructField('thumbnail_link',StringType(),True),
       StructField('comments_disabled',StringType(),True),
       StructField('ratings_disabled',StringType(),True),
       StructField('video_error_or_removed',StringType(),True)
       
       ]
    finalStruct=StructType(fields=newDF)
   # data = spark.read.format("csv").option("header","true").load('USvideos.csv')
    data = spark.read.csv("USvideos.csv", header=True, schema=finalStruct)
    return data

data = load_data()  
@st.cache
def load_data():
  data_load_state.text("(using st.cache)")
def main():  
  #st.table(data.toPandas().sample(n=10))
  if st.button("Videos that received most comments by users"):
    from pyspark.sql import functions as F
    df2=data.groupBy("Title").agg(F.max("comment_count"))
    df1=df2.orderBy('max(comment_count)', ascending=False)
    #st.table(df1.toPandas().head(11))
    x_values=df1.toPandas()["Title"].head(11)
    y_values=df1.toPandas()["max(comment_count)"].head(11)
    fig = px.bar(data_frame=df1.toPandas(), x=x_values, y=y_values)
  
    #fig.update_layout(xaxis_tickangle=90,width=800,height=800)
    fig.update_layout(xaxis_tickangle=90,width=800,height=800,
      title='Max comment count',
      xaxis_tickfont_size=14,
      yaxis=dict(
          title='Comments (millions)',
          titlefont_size=16,
          tickfont_size=14,
      ),
      xaxis=dict(
          title='YT title',
          titlefont_size=16,
          tickfont_size=14,
      ))
    st.plotly_chart(fig)
  

  if st.button("Videos posted in the year 2017 and 2018 on YouTube"):
    #st.subheader('Videos Publish in Year 2017 and 2018')
    data.createOrReplaceTempView("EMP")
    q1=spark.sql("SELECT SUM(IF(year(publish_time) = '2017', 1, 0)) AS Yr2017, SUM(IF(year(publish_time) = '2018', 1, 0)) AS Yr2018 FROM EMP") 
    #st.table(q1.toPandas())
  
    x1=q1.toPandas()['Yr2017'].iloc[0]
    y1=q1.toPandas()["Yr2018"].iloc[0]
    #fig = px.bar(data_frame=q1.toPandas(), x=x,y=y)
    #fig.update_layout(title_text='Vidoes Publish in Year 2017 and 2018')
    #st.plotly_chart(fig)
  
    import plotly.graph_objects as go
    import matplotlib.pyplot as plt
    labels = ['2017','2018']
    values = [x1, y1]
    explode = (0, 0.1)

    #st.pyplot(fig)
    fig1, ax1 = plt.subplots()
    ax1.pie(values, explode=explode, labels=labels, autopct='%1.1f%%',
          shadow=True, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
  
    st.pyplot(fig1)
    st.set_option('deprecation.showPyplotGlobalUse', False)
  if st.button("Performance of three YouTube channels based on likes, dislikes, and comment count"):
    #st.subheader('Compare 3 video production yt channel')
    data.createOrReplaceTempView("EMP")
 
    a1=spark.sql("SELECT trending_date,title,channel_title, views,likes,dislikes,comment_count,thumbnail_link  from EMP WHERE channel_title like 'Warner Bros. Pictures' order by views desc")
    a2=spark.sql("SELECT trending_date,title,channel_title, views,likes,dislikes,comment_count,thumbnail_link  from EMP WHERE channel_title like 'Netflix' order by views desc ")
    a3=spark.sql("SELECT trending_date,title,channel_title, views,likes,dislikes,comment_count,thumbnail_link  from EMP WHERE channel_title like 'HBO' order by views desc ")
    import plotly.graph_objects as go
    y11=a1.toPandas()['likes'].iloc[0]
    y2=a1.toPandas()["dislikes"].iloc[0]
    y3=a1.toPandas()["comment_count"].iloc[0]
    y111=a2.toPandas()['likes'].iloc[0]
    y22=a2.toPandas()["dislikes"].iloc[0]
    y33=a2.toPandas()["comment_count"].iloc[0]
    y1111=a3.toPandas()['likes'].iloc[0]
    y222=a3.toPandas()["dislikes"].iloc[0]
    y333=a3.toPandas()["comment_count"].iloc[0]
  
    rows = ['Likes', 'Dislikes', 'Comment count']

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=rows,
        y=[y11,y2,y3],
        name='Warner Bros',
        marker_color='indianred'
  ) )
    fig.add_trace(go.Bar(
        x=rows,
        y=[y111,y22,y33],
        name='Netflix',
        marker_color='lightsalmon'
    ))
    fig.add_trace(go.Bar(
        x=rows,
        y=[y1111,y222,y333],
        name='HBO',
        marker_color='Orange'
    ))

    # Here we modify the tickangle of the xaxis, resulting in rotated labels.
    fig=fig.update_layout(barmode='group')
    st.plotly_chart(fig)


  
  if st.button("WordCloud: Frequency of videos uploaded by various YouTube channels"):
    #st.subheader('WordCloud')
    data.createOrReplaceTempView("EMP")
    df5=spark.sql("select video_error_or_removed, concat_ws(',', collect_list(channel_title)) as channel_title from EMP group by video_error_or_removed")
    df6=df5.select('channel_title').collect()
    pandadf5=df5.toPandas()
    from wordcloud import WordCloud
    from matplotlib import pyplot as plt
    import matplotlib
    #import tkinter
    text = pandadf5.channel_title[0]
    #matplotlib.use('TkAgg')
   # Create and generate a word cloud image:
    wordcloud = WordCloud().generate(text)

  # Display the generated image:  
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    st.pyplot()
 
  

  if st.button("Made by"):

        st.text("Adil Ansari")
        st.text("Jovin Jose")
        st.text("Shrutik Kupekar")
        st.text("Aadon Melath")


if __name__ == '__main__':
	main() 

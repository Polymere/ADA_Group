import pandas as pd
import seaborn as sns
from pyspark import RDD
from analysis_functions import is_numeric
import matplotlib.pyplot as plt


def plot_violin(lst,name_feature,is_hot_threshold=0.7,keep_zero_feat=False):
    feature_lst=[]
    year_lst=[]
    hotness_lst=[]

    for song in lst:
        if song[1][0][0] !=0 or keep_zero_feat==True:
            feature_lst.append(song[1][0][0])
            year_lst.append(song[1][0][1])
            hotness_lst.append(song[1][1])

    df=pd.DataFrame()
    df['Year']=year_lst
    df[name_feature]=feature_lst
    df['Hotness']=hotness_lst
    df['is_hot']=df.Hotness>is_hot_threshold
    sns.set(style="whitegrid", palette="pastel", color_codes=True)
    sns.set_style('ticks')
    fig, ax = plt.subplots()
    fig.set_size_inches(100, 5)
    #yearSpan=df.Year.max()-df.Year.min()
    ##for year in df.Year:
    sns.violinplot( x='Year', 
                        y=name_feature,
                        hue='is_hot',
                        data=df,
                        split=True)
    sns.despine()
    
    
def rdd_to_df(rdd,year,hotness,name_feature,is_hot_threshold=0.7,keep_zero_feat=True,numeric=False):
    if numeric:
        feat=rdd.map(lambda x: (x[0], x[1][name_feature][0])).filter(lambda x: is_numeric(x[1]))
    else:
        feat=rdd.map(lambda x: (x[0], x[1][name_feature][0]))
    joined=feat.join(year).join(hotness)
    lst=joined.collect()
    feature_lst=[]
    year_lst=[]    
    hotness_lst=[]
    for song in lst:
        if song[1][0][0] !=0 or keep_zero_feat==True:
            feature_lst.append(song[1][0][0])
            year_lst.append(song[1][0][1])
            hotness_lst.append(song[1][1])
    df=pd.DataFrame()
    df['Year']=year_lst
    df[name_feature]=feature_lst
    df['Hotness']=hotness_lst
    df['is_hot']=df.Hotness>is_hot_threshold
    return df

def get_n_most_common(df,name_feature,n=1):
    df['Year_cp']=df.Year
    gby=df.groupby('Year')
    t=gby[name_feature].value_counts()
    year_idx=t.index.levels[0]
    s=pd.Series(index=year_idx)
    for idx in year_idx:
        v=t[idx].nlargest(n=n)
        s[idx]=v.index[0]
    return s
#!/usr/bin/env python
# coding: utf-8

# In[10]:


import pandas as pd
df = pd.read_csv(r"C:\Users\batch1\Downloads\logs_dataset.csv")
print(df)


# In[11]:


df['@timestamp'] = (
    df['@timestamp']
    .str.replace(r'(\d+)(st|nd|rd|th)', r'\1', regex=True)
)
df['@timestamp'] = pd.to_datetime(
    df['@timestamp'],
    format='%B %d %Y, %H:%M:%S.%f'
)
print(df)


# In[13]:


df.sort_values(['ip_address', '@timestamp'], inplace=True)
print(df)


# In[14]:


df['shift_time'] = df.groupby(['ip_address'])['@timestamp'].shift(1)
print(df)


# In[15]:


df.head()


# In[16]:


df['time_diff'] = (df['@timestamp']-df['shift_time']).dt.seconds//60
print(df)


# In[17]:


df['date'] = df['@timestamp'].dt.date
print(df)


# In[18]:


df['weekday'] = df['@timestamp'].dt.weekday
print(df)


# In[19]:


df['hour'] = df['@timestamp'].dt.hour
print(df)


# In[20]:


df['is_weekend'] = ((df['weekday'] == 5) | (df['weekday'] == 6)).astype(int)
print(df)


# In[21]:


df['hour_bucket'] = df['hour']//4
print(df)


# In[31]:


ip_addr ='ip_address'
print(ip_addr)


# In[30]:


ip_counts = df.groupby(ip_addr)['@timestamp'].count().reset_index()
print(ip_counts)


# In[25]:


ip_counts.head()


# In[28]:


ip_counts = ip_counts.rename(columns={'@timestamp':'total_count'})
print(ip_counts)


# In[29]:


daily_counts = df.groupby([ip_addr,'date'])['@timestamp'].count().reset_index()
print(daily_counts)


# In[32]:


daily_counts = daily_counts.rename(columns={'@timestamp':'daily_counts'})
print(daily_counts)


# In[33]:


daily_counts_avg = daily_counts.groupby(ip_addr).daily_counts.median().reset_index()
print(daily_counts_avg)


# In[34]:


daily_counts_avg.head(5)


# In[35]:


weekend_counts = df.groupby([ip_addr, 'is_weekend'])['@timestamp'].count().reset_index()
print(weekend_counts)


# In[ ]:





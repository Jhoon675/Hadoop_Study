# 1. HDFS에 접속 -> 결고 파일 읽기
# 2. 결과 내용을 DataFrame으로 변환
# 3. Matplotlib으로 시각화

# 1번 작업
from hdfs import InsecureClient
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# 문자열을 input output 파일 처럼 받을 수 있다.
from io import StringIO

client = InsecureClient("http://192.168.56.100:50070", user = "hadoop")
# 접속 확인
# print(client)

# 데이터 읽어오기
with client.read("output/dept_delay_count/part-r-00000", encoding="utf-8") as reader:
    data = reader.read();

# print(data)
# 현재 data는 파일이 아니다
# data -> str -> stream

stream = StringIO(data)
# print(stream)
df = pd.read_csv(stream, sep="\t", header=None)
# print(df)
# 년과 월을 분리
# print(df[0].str.split(","))
df['year'] = df[0].str.split(",").str[0]
df['month'] = df[0].str.split(",").str[1]
# print(df)

# year, month 컬럼을 int로 변환
df['year'] = df['year'].astype("int")
df['month'] = df['month'].astype("int")


# 데이터 프레임을 year을 기준으로 group
grouped_df = df.groupby(by="year")

# 시각화
fig, ax = plt.subplots() # 여러개의 플롯을 겹치기 위한 서브 플롯
for year, df in grouped_df:
    # print(year)
    # print(df.dtypes)
    sorted_df = df.sort_values(by="month", axis=0)
    ax.plot(sorted_df['month'], sorted_df[1], label=year, marker="*")

plt.legend(loc="upper right")
plt.show()
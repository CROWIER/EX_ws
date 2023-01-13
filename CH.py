#
import numpy as np
import pandas as pd
import psycopg2 as psycopg2

connection = psycopg2.connect(
    host="expertonica-dev-rds.cmmavsb2ijcp.eu-north-1.rds.amazonaws.com",
    database="okkam_db",
    user="okkam_admin",
    password="xwzKdwhNrQf1LfV#")

dtypes = np.dtype(
    [
        ("doc_id", str),
        ("site_id", str),
        ("www", str),
        ("contains?", bool),
        ("count", int)
    ]
)
df2 = pd.DataFrame(np.empty(0, dtype=dtypes))
df = pd.DataFrame(pd.read_excel('urls.xlsx'))
df.rename(columns={'N': 'doc_id'}, inplace=True)
df['doc_id'] = df['doc_id'].astype(str)
df3 = pd.read_sql_query('select site_id, doc_id, count(distinct netloc) as count_netloc from html_db2 group by site_id, doc_id having count(distinct netloc) != 1',
                        con=connection)
df3['doc_id'] = df3['doc_id'].astype(str)
#df4 = pd.merge(df, df3, how='outer', suffixes = ('_left', '_right'), on='doc_id')
print(df3)
#df4.columns = ["site_id", "doc_id", "count"]
df3.to_excel("list3.xlsx")
# docs = df.values.tolist()
# count = 0
# count2 = 0
# for i in docs:
#     if any(str(i[0]) in sublist for sublist in doc) and doc[count][0] == i[0]:
#         df2 = df2.append(
#             {'doc_id': str(i[0]), 'site_id': str(i[1]), 'www': str(i[2]), 'contains?': 'True', 'count': doc[count][1]},
#             ignore_index=True)
#         print(df2)
#         count += 1
#     else:
#         df2 = df2.append(
#             {'doc_id': str(i[0]), 'site_id': str(i[1]), 'www': str(i[2]), 'contains?': 'False', 'count': 0},
#             ignore_index=True)
#
#     print(df2)
#
# print(df2)
# df2.to_excel("list2.xlsx")

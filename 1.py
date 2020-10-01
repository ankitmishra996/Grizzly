import grizzly
import sqlite3
from grizzly.relationaldbexecutor import RelationalExecutor
from grizzly.wrapper import wrapper
from grizzly.aggregates import AggregateType


con=wrapper.wrap(["./sample.xlsx","./sample.csv","./grizzly.db"])
grizzly.use(RelationalExecutor(con))


print("----------------------------------------")
df1 = grizzly.read_table("t1")
df2 = grizzly.read_table("t2")

#j  = df1.join(df2, on = (df1.actor1name == df2.actor2name) | (df1["actor1countrycode"] <= df2["actor2countrycode"]), how="left outer")
j  = df1.join(df2, on = (df1.actor1name == df2.actor2name) , how="inner")
print(j.generate())
j.show(pretty=True)




print("---------------Show DB data-------------------------")
dfdb1 = grizzly.read_table("t1")
dfdb2 = grizzly.read_table("t2")
print("---------------Join Between t2 and t1-------------------------")
#j3  = dfdb1.join(dfdb2, on = (dfdb1.actor1name == dfdb2.actor2name) , how="inner")

j3= dfdb1.join(dfdb2, on=(dfdb1.actor1name == dfdb2.actor2name), how="inner")
print(j3.generate())
j3.show(pretty=True)



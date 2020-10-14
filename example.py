import grizzly
import sqlite3
from grizzly.relationaldbexecutor import RelationalExecutor
#from grizzly.wrapper import wrapper
from grizzly.aggregates import AggregateType

conn = sqlite3.connect('file::memory:?cache=shared')
grizzly.use(RelationalExecutor(conn,["./sample.csv","./sample.xlsx","./grizzly.db"]))

print("----------------show Excel Data sheet1------------------------")
dfexcel = grizzly.read_table("sample.xlsx.Sheet1")
dfexcel.show(pretty=True)

print("----------------Aggregate count function on excel------------------------")
g= dfexcel.groupby(["Country","Product"])
a = g.agg(col="Product",aggType=AggregateType.COUNT)

print(a.generate())
a.show(pretty=True)

print("---------------show Excel Data sheet2-------------------------")
dfexcel2 = grizzly.read_table("sample.xlsx.Sheet2")
dfexcel2.show(pretty=True)

print("---------------join between two excel sheet-------------------------")
j= dfexcel.join(dfexcel2, on=(dfexcel.Segment == dfexcel2.Segment), how='inner')
print(j.generate())
j.show(pretty=True)

print("---------------Show csv data-------------------------")
dfcsv = grizzly.read_table("sample.csv.sample")
dfcsv.show(pretty=True)


print("---------------Aggregate SUM on CSV File-------------------------")
j1= dfcsv.groupby(["Segment"])
a2 = j1.agg(col="MonthNumber",aggType=AggregateType.SUM)

print(a2.generate())
a2.show(pretty=True)

print("---------------projection and filter on csv-------------------------")
j9 = dfcsv[["Segment","MonthNumber"]] 
print(j9.generate())
j9.show(pretty=True)

j10 = dfcsv[dfcsv["Segment"] == "Government"] 
print(j10.generate())
j10.show(pretty=True)

print("---------------Join between Excel and csv-------------------------")
j2= dfexcel.join(dfcsv, on=(dfexcel.Segment == dfcsv.Segment), how="inner")

print(j2.generate())
j2.show(pretty=True)

print("---------------Show DB data-------------------------")
dfdb = grizzly.read_table("grizzly.db.sample")
dfdb.show(pretty=True)

print("---------------Join Between db and csv-------------------------")
j4= dfdb.join(dfcsv, on=(dfdb.Segment == dfcsv.Segment), how="left outer")

print(j4.generate())
j4.show(pretty=True)

print("--------------Join between db and Excel--------------------------")
j5= dfdb.join(dfexcel, on=(dfdb.Segment == dfexcel.Segment), how="inner")

print(j5.generate())
j5.show(pretty=True)

print("---------------Join Between two different tables t2 and t1-------------------------")
dfdb1 = grizzly.read_table("grizzly.db.t1")
dfdb2 = grizzly.read_table("grizzly.db.t2")
dfdb3 = grizzly.read_table("grizzly.db.events")

print("---------------left outer Join Between two different tables t2 and t1-------------------------")

j8 = dfdb1.join(dfdb2, on = (dfdb1.actor1name == dfdb2.actor2name) | (dfdb1["actor1countrycode"] <= dfdb2["actor2countrycode"]), how="left outer")
print(j8.generate())
j8.show(pretty=True)

print("---------------Inner Join Between two different tables t2 and t1-------------------------")

j3= dfdb1.join(dfdb2, on=(dfdb1.actor1name == dfdb2.actor2name), how="inner")
print(j3.generate())
j3.show(pretty=True)

print("---------------Aggregate count on db table t1-------------------------")

j6= dfdb1.groupby(["actor1name","actor1countrycode"])
a6 = j6.agg(col="actor1countrycode",aggType=AggregateType.COUNT)

print(a6.generate())
a6.show(pretty=True)

print("---------------projection and filter on db table ""events""-------------------------")
j7 = dfdb3[["actor1name","actor2name"]] 
print(j7.generate())
j7.show(pretty=True)

j8 = dfdb3[dfdb3["globaleventid"] == 470747760] 
print(j8.generate())
j8.show(pretty=True)




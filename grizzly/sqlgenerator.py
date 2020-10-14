from grizzly.aggregates import AggregateType
from grizzly.dataframes.frame import Table, Projection, Filter, Join, Grouping, DataFrame
from grizzly.expression import ColRef, Expr

from grizzly.generator import GrizzlyGenerator

import random
import string
import sqlite3
import pandas as pd
import os
import warnings
warnings.filterwarnings('ignore')


class Query:

  def __init__(self):
    self.filters = []
    self.projections = None
    self.doDistinct = False
    self.table = None
    self.groupcols = []
    self.groupagg = set()
    self.joins = []

  def _reset(self):
    self.filters = []
    self.projections = None
    self.doDistinct = False
    self.table = None
    self.groupcols = []
    self.groupagg = set()
    self.joins = []

  def _doExprToSQL(self, expr):
    exprSQL = ""
    # right hand side is a string constant
    if isinstance(expr, str):
      exprSQL = f"'{expr}'"
    # right hand side is a dataframe (i.e. subquery)
    elif isinstance(expr, DataFrame): 
      # if right hand side is a DataFrame, we need to create code first 
      subQry = Query()
      exprSQL = subQry._buildFrom(expr)

    elif isinstance(expr, ColRef):
      exprSQL = str(expr)

    elif isinstance(expr, Expr):
      l = self._doExprToSQL(expr.left)
      r = self._doExprToSQL(expr.right)

      exprSQL = f"{l} {expr.opStr} {r}"
    # right hand side is some constant (other than string), e.g. number
    else:
      exprSQL = str(expr)

    return exprSQL

  def _exprToSQL(self, expr):
    leftExpr = self._doExprToSQL(expr.left)
    rightExpr = self._doExprToSQL(expr.right)

    return f"{leftExpr} {expr.opStr} {rightExpr}"
  def insertIntoDB(self,files,table,connection,sources ):
      cursor=connection.cursor()
      matching = [s for s in sources if files in s]
      if files.lower().endswith('.csv'):
          if (matching[0]):
              row=cursor.execute("SELECT name from sqlite_master where type='table' and name='"+table+"'")
              if(row.fetchone() == None):
                  pd.read_csv(matching[0], sep=",").to_sql(table, connection, if_exists='replace', index=True)
      elif files.lower().endswith(('.xlsx','.xls')):
          row=cursor.execute("SELECT name from sqlite_master where type='table' and name='"+table+"'")
          if(row.fetchone() == None):
              df= pd.read_excel(matching[0], sheet_name=table)
              df.to_sql(table,connection)
      elif files.lower().endswith(('.db')):
          c= connection.cursor();
          c.execute('ATTACH DATABASE "'+matching[0]+'" as '+table+'')
          connection.commit()
  def _buildFrom(self,df,sources,connection):

    curr = df
    while curr is not None:

      if isinstance(curr,Table):
        nameSplit=curr.table.rsplit(".",1)
        curr.table=nameSplit[0]
        if(len(nameSplit)>1):
            self.insertIntoDB(nameSplit[0],nameSplit[1],connection,sources)
            curr.table=nameSplit[1]

        self.table = f"{curr.table} {curr.alias}"

      elif isinstance(curr,Projection):
        if curr.attrs:
          prefixed = [str(attr) for attr in curr.attrs]
          if not self.projections:
            self.projections = prefixed
          else:
            set(self.projections).intersection(set(prefixed))
        if curr.doDistinct:
          self.doDistinct = True

      elif isinstance(curr,Filter):
        exprStr = self._exprToSQL(curr.expr)
        self.filters.append(exprStr)

      elif isinstance(curr, Join):
        if isinstance(curr.right, Table):
            nameSplit=curr.right.table.rsplit(".",1)
            curr.table=nameSplit[0]
            if(len(nameSplit)>1):
                self.insertIntoDB(nameSplit[0],nameSplit[1],connection,sources)
                curr.right.table=nameSplit[1] 
            rightSQL = curr.right.table
            rtVar = curr.right.alias
        else:
          subQry = Query()
          rightSQL = f"({subQry._buildFrom(curr.right)})"
          rtVar = GrizzlyGenerator._incrAndGetTupleVar()
          # curr.right.alias = rtVar
          curr.right.setAlias(rtVar)

        if isinstance(curr.on, Expr):
          onSQL = self._exprToSQL(curr.on)
        else:
          onSQL = f"{curr.alias}.{curr.on[0]} {curr.comp} {rtVar}.{curr.on[1]}"
        
        joinSQL = f"{curr.how} JOIN {rightSQL} {rtVar} ON {onSQL}"
        self.joins.append(joinSQL)

      elif isinstance(curr, Grouping):
        self.groupcols = [str(attr) for attr in curr.groupCols]

        if curr.aggFunc:
          (func, col) = curr.aggFunc
          funcCode = SQLGenerator._getFuncCode(func, col, curr) 
          self.groupagg.add(funcCode)

      if curr.parents is None:
        curr = None
      else:
        curr = curr.parents[0]

    joins = ""
    while self.joins:
      joins += " "+self.joins.pop()
    
    projs = "*"
    if self.projections:
      if self.groupcols and not set(self.projections).issubset(self.groupcols):
        raise ValueError("Projection list must be subset of group columns")

      projs = ', '.join(self.projections) 

    grouping = ""
    if self.groupcols:
      theColRefs = ", ".join([str(e) for e in self.groupcols])
      grouping += f" GROUP BY {theColRefs}"

      if projs == "*":
        projs = theColRefs

    if len(self.groupagg) > 0:
      if projs == "*":
        projs = self.groupagg
      elif len(self.groupagg) > 0:
        projs = projs + "," + ",".join(self.groupagg)

    if self.doDistinct:
      projs = "distinct " + projs

    where = ""
    if len(self.filters) > 0:
      exprs = " AND ".join([str(e) for e in self.filters])
      where += f" WHERE {exprs}"

    qrySoFar = f"SELECT {projs} FROM {self.table}{joins}{where}{grouping}"
    return qrySoFar

class SQLGenerator:

  @staticmethod
  def _getFuncCode(func, col, df):
    if not isinstance(col, ColRef):
      colName = ColRef(col, df)
    else:
      colName = col
    
    if func == AggregateType.MEAN:
      funcStr = "avg"
    else:
      funcStr = str(func).lower()[len("aggregatetype."):]

    funcCode = f"{funcStr}({colName})"
    return funcCode

  def generate(self, df, sources,connections):
    qry = Query()
    return qry._buildFrom(df,sources,connections)

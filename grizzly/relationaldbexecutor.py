from grizzly.expression import ColRef
from grizzly.sqlgenerator import SQLGenerator
from grizzly.generator import GrizzlyGenerator

class RelationalExecutor(object):
  
  def __init__(self, connection, sources ,sqlGenerator=SQLGenerator()):
    super().__init__()
    self.source = sources
    self.connection = connection
    self.sqlGenerator = sqlGenerator

  def generate(self, df):
    return self.sqlGenerator.generate(df,self.source,self.connection)

  def _execute(self, sql):
    cursor = self.connection.cursor()
    try:
      cursor.execute(sql)
    except Exception as e:
      print("Failed to execute query. Reason: {e}")
      print(f"Query: {sql}")
    finally:
      return cursor  
    

  def close(self):
    self.connection.close()

  def collect(self, df, includeHeader):
    rs = self.execute(df)

    tuples = []

    if includeHeader:
      cols = [dec[0] for dec in rs.description]
      tuples.append(cols)

    for row in rs:
      tuples.append(row)

    return tuples


  def table(self,df):
    rs = self.execute(df)
    import beautifultable
    table = beautifultable.BeautifulTable()
    for row in rs:
      table.append_row(row)

    rs.close()
    return str(table)

  def toString(self, df, delim=",", pretty=False, maxColWidth=20, limit=20):
    rs = self.execute(df)

    cols = [dec[0] for dec in rs.description]
    

    if not pretty:
      strings = [delim.join(cols)]
      cnt = 0
      for row in rs:
        if limit is None or cnt < limit:
          strings.append(delim.join([str(col) for col in row]))
        cnt += 1

      rs.close()

      if  limit is not None and cnt > limit and cnt - limit > 0:
        strings.append(f"and {cnt - limit} more...")

      return "\n".join(strings)
    else:
      firstRow = rs.fetchone()

      colWidths = [ min(maxColWidth, max(len(x),len(str(y)))) for x,y in zip(cols, firstRow)]

      rowFormat = "|".join([ "{:^"+str(width+2)+"}" for width in colWidths])
      

      def formatRow(theRow):
        values = []
        for col, colWidth in zip(theRow, colWidths):
          strCol = str(col)
          if len(strCol) > colWidth:
            values.append(strCol[:(colWidth-3)]+"...")
          else:
            values.append(strCol)

        return rowFormat.format(*values)

      resultRep = [formatRow(cols), formatRow(firstRow)]
      cnt = 0
      for row in rs:
        if  limit is None or cnt < limit:
          resultRep.append(formatRow(row))

        cnt += 1

      rs.close()

      if limit is not None and cnt > limit and cnt - limit > 0:
        resultRep.append(f"and {cnt - limit} more...")

      return "\n".join(resultRep)

  def execute(self, df):
    """
    Execute the operations and print results to stdout

    Non-pretty mode outputs in CSV style -- the delim parameter can be used to 
    set the delimiter. Non-pretty mode ignores the maxColWidth parameter.
    """
    
    sql = self.sqlGenerator.generate(df,self.source,self.connection)
    return self._execute(sql)

  def _execAgg(self, df, func, col):
    """
    Actually compute the aggregation function.

    If we have a GROUP BY operation, the aggregation is only stored
    as a transformation and needs to be executed using show() or similar.

    If no grouping exists, we want to compute the aggregate over the complete
    table and return the scalar result directly
    """

    # if isinstance(df, Grouping):
    #   newOp = Grouping(self.op.groupcols, self.op.parent)
    #   newOp.setAggFunc(funcCode)
      
    #   return DataFrame(self.columns, newOp)
      
    # else:
    return self._doExecAgg(func, col, df)

  def _gen_agg(self, func, col, df):
    
    # aggregation over a table is performed in a way that the actual query
    # that was built is executed as an inner query and around that, we 
    # compute the aggregation

    if df.parents:
      innerSQL = self.generate(df)
      df.alias = GrizzlyGenerator._incrAndGetTupleVar()
      funcCode = SQLGenerator._getFuncCode(func, col, df)
      aggSQL = f"SELECT {funcCode} FROM ({innerSQL}) as {df.alias}"
      # aggSQL = innerSQL
    else:
      funcCode = SQLGenerator._getFuncCode(func, col, df)
      aggSQL = f"SELECT {funcCode} FROM {df.table} {df.alias}"

    return aggSQL

  def _doExecAgg(self, func, col, df):
    """
    Really executes the aggregation and returns the single result
    """
    aggSQL = self._gen_agg(func, col, df)
    # execute an SQL query and get the result set
    rs = self._execute(aggSQL)
    #fetch first (and only) row, return first column only
    return rs.fetchone()[0]  

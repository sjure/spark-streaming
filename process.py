import sys
from pyspark.sql import Row,SQLContext
import traceback



def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
        return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # obtén el contexto spark sql singleton desde el contexto actual

        sql_context = SQLContext(rdd.context)
        # convierte el RDD a Row RDD
        if len(rdd.take(1)) == 0:
            return
        row_rdd = rdd.map(lambda w: Row(word=w[0], word_count=w[1]))
        
        words_df = sql_context.createDataFrame(row_rdd)

        words_df.registerTempTable("words")

        word_count_df = sql_context.sql("select word, word_count from words order by word_count desc")
        word_count_df.show()

    except:
        e = sys.exc_info()
        print(e[0], e[1], e[2])
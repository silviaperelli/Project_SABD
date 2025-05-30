from Q1.query1_df import *
from Q1.query1_rdd import *
from Q1.query1_sql import *

from Q2.query2_df  import *
#from Q2.query2_rdd import *
from Q2.query2_sql import *

from Q3.query3_df import *
#from Q3.query3_rdd import *
from Q3.query3_sql import *


def test_query1():
    print("\n--------Query 1--------")
    num_executor = ["1","2","3","4"]
    for i in num_executor:
        query1_df(i)
        query1_rdd(i)
        query1_sql(i)

    print("Test completati")

def test_query2():
    print("\n--------Query 2--------")
    num_executor = ["1", "2", "3", "4"]
    for i in num_executor:
        query2_df(i)
        #query2_rdd(i)
        query2_sql(i)

    print("Test completati")

def test_query3():
    print("\n--------Query 3--------")
    num_executor = ["1", "2", "3", "4"]
    for i in num_executor:
        query3_df(i)
        #query3_rdd(i)
        query3_sql(i)

    print("Test completati")



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Devi passare il numero della query da eseguire.")
        sys.exit(1)

    scelta = sys.argv[1]

    if scelta == "1":
        test_query1()
    elif scelta == "2":
        test_query2()
    elif scelta == "3":
        test_query3()
    elif scelta == "4":
        print("query 4 da fa")
        #test_query4()
    else:
        print("Scelta non valida.")



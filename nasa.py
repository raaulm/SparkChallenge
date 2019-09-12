from pyspark import SparkConf, SparkContext
from operator import add

conf = (SparkConf().setMaster("local").setAppName("NASA").set("spark.executor.memory", "4g"))

sc = SparkContext(conf=conf)

data_logs = sc.textFile(r"C:\Desafio_Semantix\nasa_data\*")
data_logs.cache()

print("#######################################################################")
print("##########################   INICIO  ##################################")
print("#######################################################################")

unique_hosts = data_logs.map(lambda line: line.split(' ')[0]).distinct().count()
print("Total unique hosts: " + str(unique_hosts))


def request():

    mapping = data_logs.map(lambda line: line.split(' '))
    num_errors = mapping.filter(lambda line: "404" in line)
    print("Total 404 errors: " + str(num_errors.count()))

request()


def total_bytes(df_bytes):
    def countbytes(line):
        try:
            count = int(line.split(" ")[-1])
            if count < 0:
                raise ValueError()
            return count
        except:
            return 0

    count = df_bytes.map(countbytes).reduce(add)
    return count


print("Total bytes: " + str(total_bytes(data_logs)))

sc.stop()

print("#######################################################################")
print("##########################   FIM  #####################################")
print("#######################################################################")

# ----------------------------------------------------------
# EXECUTIONS
# ----------------------------------------------------------
# Todos os resultados: spark-submit nasa_logs.py request
# ----------------------------------------------------------
# END
# ----------------------------------------------------------

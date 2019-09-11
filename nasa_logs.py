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


def request(line):
    try:
        errors = line.split(' ')[-2]
        if errors == '404':
            return True
    except:
        pass
    return False


num_errors = data_logs.filter(request).cache()
print("Total 404 errors: " + str(num_errors.count()))


def url_errors(df_logs):
    host = df_logs.map(lambda line: line.split(' ')[0])
    count_errors = host.map(lambda host: (host, 1)).reduceByKey(add)
    five_hosts = count_errors.sortBy(lambda pair: -pair[1]).take(5)

    for host, count in five_hosts:
        print(host, count)

    return five_hosts


url_errors(num_errors)


def qtd_errors_day(df_day):
    days = df_day.map(lambda line: line.split('[')[1].split(':')[0])
    counts = days.map(lambda day: (day, 1)).reduceByKey(add).collect()

    for day, count in counts:
        print(day, count)

    return counts


qtd_errors_day(num_errors)


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
# Apenas as 5 URL: spark-submit nasa_logs.py url_errors
# Apenas a qtd de erros por dia: spark-submit nasa_logs.py qtd_errors_day
# ----------------------------------------------------------
# END
# ----------------------------------------------------------

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("AutoPostSales")
sc = SparkContext(conf=conf)

raw_rdd = sc.textFile("C:/Users/aswa8/OneDrive - Queen's University/Desktop/Springboard/20 - Apache Spark/Spark-Mini-Projects/data.csv")

def extract_vin_key_value(row):
    """
    Extract vin_no, make, year and incident type from data.csv
    """
    arr_val = row.split(",")

    vin_no = arr_val[2]
    make = arr_val[3]
    year = arr_val[5]
    incident_type = arr_val[1]
    return vin_no, (make, year, incident_type)

vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))

def populate_make(values):
    sorted(values)
    output_value_list = []

    for val in values:
        if val[0].strip() != '':
            make = val[0]
        if val[1].strip() != '':
            year = val[1]
        output_value_list.append((make, year, val[2]))
    return output_value_list


enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))

def extract_make_key_value(data):
    if data[2] == "A":
        return data[0], 1
    else:
        return data[0], 0

make_kv = enhance_make.map(lambda x: extract_make_key_value(x)) 

enhance_make = make_kv.reduceByKey(lambda x, y: x + y) 
print(enhance_make.collect())

final_rdd = enhance_make.collect()

with open("final_output.txt", "w") as fh:
    for item_list in final_rdd:
        print(item_list)
        fh.write(str(item_list) + "\n")

sc.stop()
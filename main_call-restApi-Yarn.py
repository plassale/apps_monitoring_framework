import urllib2
import pyspark
import json
from pyspark.context import SparkContext

sc = SparkContext(appName="Apps-Monitoring-Yarn") 

# add a window to the request api
#http://RMHOST:8088/ws/v1/cluster/apps?startedTimeBegin=1445351681496☆tedTimeEnd=1445878421


def convert_single_object_per_line(json_list):
   	json_string = ""
	for line in json_list:
    		json_string += json.dumps(line) + "\n"
        return json_string


def parse_dataframe(json_data):
    r = convert_single_object_per_line(json_data)
    mylist = []
    for line in r.splitlines():
        mylist.append(line)
    rdd = sc.parallelize(mylist)
    df = spark.read.json(rdd)
    return df
 


jsonStrings=json.load(urllib2.urlopen("http://192.168.56.101:8088/ws/v1/cluster/apps"))


dfYarnApps=parse_dataframe(jsonStrings[u'apps'][u'app'])



#dfYarnApps.show()

#>>> dfYarnApps.printSchema()
#root
# |-- allocatedMB: long (nullable = true)
# |-- allocatedVCores: long (nullable = true)
# |-- amContainerLogs: string (nullable = true)
# |-- amHostHttpAddress: string (nullable = true)
# |-- applicationTags: string (nullable = true)
# |-- applicationType: string (nullable = true)
# |-- clusterId: long (nullable = true)
# |-- diagnostics: string (nullable = true)
# |-- elapsedTime: long (nullable = true)
# |-- finalStatus: string (nullable = true)
# |-- finishedTime: long (nullable = true)
# |-- id: string (nullable = true)
# |-- memorySeconds: long (nullable = true)
# |-- name: string (nullable = true)
# |-- numAMContainerPreempted: long (nullable = true)
# |-- numNonAMContainerPreempted: long (nullable = true)
# |-- preemptedResourceMB: long (nullable = true)
# |-- preemptedResourceVCores: long (nullable = true)
# |-- progress: double (nullable = true)
# |-- queue: string (nullable = true)
# |-- runningContainers: long (nullable = true)
# |-- startedTime: long (nullable = true)
# |-- state: string (nullable = true)
# |-- trackingUI: string (nullable = true)
# |-- trackingUrl: string (nullable = true)
# |-- user: string (nullable = true)
# |-- vcoreSeconds: long (nullable = true)


dfYarnApps.createOrReplaceTempView("YarnApps")

applicationsName = spark.sql("""
SELECT id,name,applicationType,finalStatus,(finishedTime-startedTime) as runningTime , (memorySeconds/ (finishedTime-startedTime) ) as allocatedMemory
FROM YarnApps
""")

applicationsName = spark.sql("""
SELECT * 
FROM YarnApps 
WHERE finishedTime > (
	SELECT max(finishedTime) 
	FROM YarnApps 
	WHERE name='Apps-Monitoring-Yarn')
""")


applicationsName.show()

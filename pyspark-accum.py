from pyspark import SparkContext  
from pyspark import AccumulatorParam  
  
 #creating spark context   
sc=SparkContext()  
 #custom accumulator class  
class DiffAccumulatorParam(AccumulatorParam):  
    def zero(self, value):  
        dict1={}  
        for i in range(0,len(value)):  
                    dict1[i]=0  
        return dict1  
    def addInPlace(self, val1, val2):  
       for i in val1.keys():
           val1[i] += val2[i]  
       return val1
    
 #input vectors 
c={1:10,2:20,3:30,4:40}  
d=[{1:1,2:2,3:3,4:4},{1:2,2:3,3:4,4:1}]

 rdd1=sc.parallelize(d)
 
 #creating accumulator   
va = sc.accumulator(c, DiffAccumulatorParam())  
#action to be executed on rdd in order to diff vectors  
def diff(x):  
    global va  
    va += {i:-y for i,y in x}  
rdd1.foreach(diff)   
#print the value of accumulator  
print va.value 

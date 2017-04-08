import sys
import shlex
import pandas as pd
import datetime

#['./src/process_log.py', './log_input/log.txt', './log_output/hosts.txt', './log_output/hours.txt', './log_output/resources.txt', './log_output/blocked.txt']

#['221.195.71.14', '-', '-', '[01/Jul/1995:00:00:54', '-0400]', '"GET', '/shuttle/missions/sts-71/mission-sts-71.html', 'HTTP/1.0"', '200', '13387\n']

log = open(sys.argv[1],"r")
#log = open("../log1.txt","r")

##### get top 10 hosts ####
hostTable = {}
resourceTable = {}
#count = 0
#DD/MON/YYYY:HH:MM:SS -0400]
timeTable = {}
loginTable = {}
badActorActivityTable = {}
#index = pd.date_range('01/Jul/1995', periods=2471040, freq='S')
#series = pd.Series(0,index=index)
for line in log:
    lineList = shlex.split(line, posix=False)
    time = lineList[3][1:]
    reformat_time = ''.join([time[:11], ' ', time[12:]])
    host = lineList[0]
    #good login attempt
    if lineList[-2] != '404':
        resourceName = lineList[5].split(' ')[1]
        resourceSize = 0 if lineList[-1] == '-' else lineList[-1]
        resourceSize = int(resourceSize)
        if resourceName in resourceTable:
            resourceTable[resourceName] += resourceSize
        else:
            resourceTable[resourceName] = resourceSize
        if host in loginTable:
            badActorActivityTable[host].append([line,lineList,pd.to_datetime(reformat_time)])
    #failed login attempt
    else:
        if host in loginTable:
            loginTable[host].append(reformat_time)
            badActorActivityTable[host].append([line,lineList,pd.to_datetime(reformat_time)])   
        else:
            loginTable[host] = [reformat_time]
            badActorActivityTable[host] = [line,lineList,pd.to_datetime(reformat_time)]
    if host in hostTable:
        hostTable[host] += 1
    else:
        hostTable[host] = 1
    
    #print reformat_time
    if reformat_time in timeTable:    
        timeTable[reformat_time] += 1  
    else:
        timeTable[reformat_time] = 1
        
    #if count % 100000 == 0:
    #    print count / 100000
    #count += 1
blockedList = []
for i in loginTable:
    if len(loginTable[i]) >= 3:
        #print [datetime.datetime.strptime(s.to_datetime, '%b %d %H:%M:%S %Y') for s in loginTable[i]]
        timeList = [pd.to_datetime(s) for s in loginTable[i]]
        #print timeList
        for j in range(len(timeList)):
            if j+2 < len(timeList) and timeList[j+2] - timeList[j] <= datetime.timedelta(20):
                for list in badActorActivityTable[i]: 
                    #print list
                    blockedList.append(list)
            ##could not finish blockedList

blocked_file = open(sys.arv[5],"w")
#blocked_file = open('../blocked.txt',"w")
blocked_file.writelines(str(w) for w in blockedList)
blocked_file.close()

series = pd.Series(timeTable)
hours_file = open("../hours.txt", "w")
series.index = pd.to_datetime(series.index)
#hours_file.write(series)

#series.to_csv("../series.csv")
#print series
#series.resample('S',how='sum')
#series.fillna(0)
#print series
#print series.resample('S',how='sum').fillna(0).resample('H',how="sum", loffset ="1s")

#print series.resample('H',how="sum").nlargest(10)
for row in series.resample('H',how="sum").nlargest(10).iteritems():
    hours_file.write(''.join([str(row[0]),' -0400',',',str(row[1]),'\n']))
hours_file.close()

#print series.nlargest(10)   
host_file = open(sys.argv[2],"w")
#host_file = open("../hosts.txt","w")   
host_file.writelines([host + "," + str(hostTable[host]) + "\n" for host in sorted(hostTable, key = hostTable.get, reverse = True)[:10]])
host_file.close()

resource_file = open(sys.argv[4],"w")
#resource_file = open("../resources.txt","w")
resource_file.writelines([resourceName + "\n" for resourceName in sorted(resourceTable, key = resourceTable.get, reverse = True)[:10]])
resource_file.close()

#resource_file = open(sys.argv[4],"r")
#print resource_file.read()

log.close()

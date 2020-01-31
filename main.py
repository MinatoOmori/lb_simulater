from IPython.core.debugger import Pdb
import time
import datetime
import sys
import numpy as np
import random
import simulation_reqs
import simulation_servers

def getNextIdleTime(servers, next_req, current_time):
    serverList =[]
    idleTimeList = [0]*3
    for server in servers:
        server.sync(current_time)#need to sync first at choosing
        next_finish_time = current_time
        while (True):#until server can process next req
            virtualList, last_processed, virtual_cload, virtual_gload = server.virtualSync(next_finish_time)
            if (next_req.cload <= (100-virtual_cload) and \
                (next_req.gload == "none" or virtual_gload == "none")):
                serverList.append(server)
                break
            else:
                #get next idle time
                finish_times = [req.finish for req in virtualList]
                next_finish_time = min(finish_times)
        idleTimeList[server.id] = next_finish_time

    return serverList, idleTimeList

def choose(servers, next_req, current_time, predict_interval=1, method="proposed"):
    
    serverList, idleTimeList = getNextIdleTime(servers, next_req, current_time)
    print([idleTimeList])
    
    if (method == "proposed"):
        predictList=[]
        #for server in servers:
        next_req.server_arrival = max(idleTimeList)
        for server in servers:
            if (idleTimeList[server.id] != current_time):
                #next_req.server_arrival = idleTimeList[server.id]
                predict_Tex, predict_Tcs, predict_Tc_ests, predict_Teff, before_end = server.predict_static(next_req, next_req.server_arrival)
            else:
                #next_req.server_arrival = current_time
                predict_Tex, predict_Tcs, predict_Tc_ests, predict_Teff, before_end = server.predict_static(next_req, next_req.server_arrival)
            predictList.append({'predict_Tex': predict_Tex, 'predict_Tcs': predict_Tcs, 'predict_Tc_ests': predict_Tc_ests, 'predict_Teff': predict_Teff, 'before_end': before_end})

        #maxTemp = [max(predictList[x]['predict_Tex']) for x in range(len(predictList))]
        meanTemp = [np.mean(predictList[x]['predict_Tex']) for x in range(len(predictList))]
        print("max(predict0): %f"%max(predictList[0]['predict_Tex']))
        print("max(predict1): %f"%max(predictList[1]['predict_Tex']))
        print("max(predict2): %f"%max(predictList[2]['predict_Tex']))
        minIndex = meanTemp.index(min(meanTemp))
        minServer = servers[minIndex]

        if (idleTimeList[minServer.id] == current_time):
            #if minServer is idle
            next_req.set_finish_time(current_time)#next req starts at current_time
            predict_Tex, predict_Tcs, predict_Tc_ests, predict_Teff, before_end = minServer.predict_static(next_req)
            predictList[minIndex] = {'predict_Tex': predict_Tex, 'predict_Tcs': predict_Tcs, 'predict_Tc_ests': predict_Tc_ests, 'predict_Teff': predict_Teff, 'before_end': before_end}

        else:
            #get next idle time
            next_req.set_finish_time(idleTimeList[minServer.id])#next req starts at nest_finish_time
            predict_Tex, predict_Tcs, predict_Tc_ests, predict_Teff, before_end = minServer.predict_static(next_req, idleTimeList[minServer.id])
            predictList[minIndex] = {'predict_Tex': predict_Tex, 'predict_Tcs': predict_Tcs, 'predict_Tc_ests': predict_Tc_ests, 'predict_Teff': predict_Teff, 'before_end': before_end}

        last_req_fin = next_req.finish
        end_point = 0
        for req in minServer.processing:
            if(last_req_fin < req.finish): 
                last_req_fin = req.finish
        before_end = predictList[minIndex]['before_end'] 
        start_point = next_req.finish - last_req_fin - before_end - 1
        end_point = -last_req_fin + next_req.finish

        if (end_point < 0):
            minServer.Tex[-before_end-1:end_point] = predictList[minIndex]['predict_Tex']
            for i in range(len(predict_Tcs)):
                minServer.Tcs[i][-before_end-1:end_point] = predictList[minIndex]['predict_Tcs'][i]
            for i in range(len(predict_Tc_ests)):
                minServer.Tc_ests[i][-before_end-1:end_point] = predictList[minIndex]['predict_Tc_ests'][i]
            minServer.Teff[-before_end-1:end_point] = predictList[minIndex]['predict_Teff']
        else:
            minServer.Tex[start_point:] = predictList[minIndex]['predict_Tex']
            for i in range(len(predict_Tcs)):
                minServer.Tcs[i][start_point:] = predictList[minIndex]['predict_Tcs'][i]
            for i in range(len(predict_Tc_ests)):
                minServer.Tc_ests[i][start_point:] = predictList[minIndex]['predict_Tc_ests'][i]
            minServer.Teff[start_point:] = predictList[minIndex]['predict_Teff']

    elif (method == "RR"):
        server_id = next_req.id%len(servers)
        minServer = [x for x in servers if x.id == server_id][0]
        s_cload, s_gload = minServer.getLoad()
        if (next_req.cload <= (100-s_cload) and \
            (next_req.gload == "none" or s_gload == "none")):
            next_req.set_finish_time(current_time)#next req starts at current_time
        else:
            #get next idle time
            finish_times = [req.finish for req in minServer.processing]
            if (next_finish_time == 0):
                next_finish_time = min(finish_times)
            elif (next_finish_time > min(finish_times)):
                next_finish_time = min(finish_times)
            next_req.set_finish_time(next_finish_time)#next req starts at nest_finish_time
        
    elif (method == "cload"):
        next_cload=[]
        for i, serverIdleTime in enumerate(idleTimeList):
            if (serverIdleTime != current_time):
                virtualList, last_processed, s_cload, s_gload = servers[i].virtualSync(serverIdleTime)
                next_req.server_arrival = serverIdleTime
            else:
                next_req.server_arrival = current_time
                s_cload, s_gload = servers[i].getLoad()
            next_cload.append(s_cload)

        minIndex = [i for i, v in enumerate(next_cload) if v == min(next_cload)]
        candidate_servers = [x for x in servers if x.id in minIndex]
        minServer = sorted(candidate_servers, key=lambda x: idleTimeList[x.id])[0]#first end server in min cloads

        if (idleTimeList[minServer.id] == current_time):
            #if minServer is idle
            next_req.set_finish_time(current_time)#next req starts at current_time

        else:
            #get next idle time
            next_req.set_finish_time(idleTimeList[minServer.id])#next req starts at nest_finish_time

    return minServer

def main():
    file_time = datetime.datetime.now()
    filepath = "/home/minato/Documents/master/LB_simulater/results/results_{0:%Y%m%d_%H%M}.txt".format(file_time)

    N_req = int(sys.argv[1])
    N_server = int(sys.argv[2])
    
    method = sys.argv[3]
    infile_path = sys.argv[4]
    flagRand = False
    if (infile_path is None):
        print(test)
        flagRand = True
    #N_req = 5
    #N_server = 2

    if (flagRand):
        ctime_min = 10
        ctime_max = 50
        interval_min = 5
        interval_max = 15
    else:
        input_file = open(infile_path, 'r')

    servers=[]
    reqs=[]
    lbQueue = []
    
    current_time = 0
    
    for i in range(N_server):
        #generate servers
        servers.append(simulation_servers.Server(i))
    
    result_file = open(filepath, mode='w')
    result_file.write("req_id, LB_arrival, server_arrival, allocated_server, process_time, finish\n")
    count = 0
    for i in range(N_req):
        
        #generate requests
        if (flagRand):
            #cload = random.randint(10, 100)
            cloadList = [0, 50, 100]
            cload = random.choice(cloadList)
            ctime = random.randint(ctime_min, ctime_max)
            gloadList = ["none", "burn", "matrix"]
            if (cload == 0):
                gloadList = ["burn", "matrix"]
            gload = random.choice(gloadList)
            interval = random.randint(interval_min, interval_max)
        else:
            cloadList = [0, 50, 100]
            ctime_max = 143
            cload = random.choice(cloadList)
            gloadList = ["none", "burn", "matrix"]
            if (cload == 0):
                gloadList = ["burn", "matrix"]
            gload = random.choice(gloadList)
            input_line = input_file.readline()
            input_list = input_line.rstrip().split(',')
            ctime = int(float(input_list[5]))
            if (i == 0):
                interval = 0
            else:
                interval = int(round(float(input_list[1])-pre_real_start))
            pre_real_start = float(input_list[1])

        req = simulation_reqs.Request(cload, ctime, gload, current_time+interval, i)
        reqs.append(req)
        
        #check LB queue
        if not(lbQueue):
            current_time = current_time + interval
            next_req = req
        else:
            lbQueue.append(req)
            next_req = lbQueue.pop(0)
            
        #choose server
        next_server =  choose(servers, next_req, current_time, method=method)

        print ("next_server: %i"%next_server.id)
        print (next_req.finish)
        #print (next_server.Tex)
        result_file.write("%i, %i, %i, %i, %i, %i\n"%(next_req.id, next_req.lb_arrival, next_req.server_arrival, next_server.id, next_req.ctime, next_req.finish))
        
        #allocate server and execute in servers[0]
        if (count == 0):
            start_time = datetime.datetime.now()

        count = next_server.allocate(next_req, count)

    #should wait until all finish in servers[0]
    #result_file.write("server{0} process start: {1}\n".format(0, start_time))
    #result_file.write("number of requests: %i\n"%count)
    #result_file.write("predicted temperature: \n{0}\n".format(servers[0].Tex))
    #print(servers[0].Tex)
    #end_time = datetime.datetime.now()
    #result_file.write("server{0} process end: {1}\n".format(0, end_time))
    #time.sleep(ctime_max)

    #execute in rest servers
    for server in servers:
        count = 0
        time.sleep(300) #wait until oak cool down 
        count, start_time = server.allocate_later(reqs, count)
        result_file.write("server{0} process start: {1}\n".format(server.id, start_time))
        result_file.write("number of requests: %i\n"%count)
        result_file.write("predicted temperature: \n{0}\n".format(server.Tex))
        print(server.Tex)
        end_time = datetime.datetime.now()
        result_file.write("server{0} process end: {1}\n".format(server.id, end_time))
        time.sleep(ctime_max)

    result_file.close()
        
if __name__ == "__main__":
    main()

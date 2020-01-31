from IPython.core.debugger import Pdb
import MySQLdb
import time
import datetime
import numpy as np
import paramiko
import random
import simulation_reqs
from oak_parameters import *
from password import *

def oak_execute(command, load, exe_t):

    R_HOST_PASS=OAK_PASS
    error_flg = False
    msg = ''
    try:
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(\
                    R_HOST, username=R_HOST_USER, password=R_HOST_PASS, \
                    port=22, timeout=15.0,look_for_keys=False)

        ssh_transport=ssh.get_transport()
        ssh_channel=ssh_transport.open_session()
        ssh_channel.settimeout(exe_t)

        if (command=="cpu1"):
            #cmd="/home/oomori/cpufan/stress_ng.sh %s %s & \n"%(exe_t, load)
            cmd="stress-ng -c 8 -t %s --taskset 0,1,2,3,4,5,6,7 -l %s -q & \n"\
            %(exe_t, load)
            ssh_channel.exec_command(cmd)

        elif (command=="cpu2"):
            cmd="stress-ng -c 8 -t %s --taskset 8,9,10,11,12,13,14,15 -l %s -q & \n"\
            %(exe_t, load)
            print (cmd)
            ssh_channel.exec_command(cmd)
        elif (command=="burn"):
            cmd="cd /home/oomori/stress_gpu/gpu-burn; ./gpu_burn %s & \n" %exe_t
            ssh_channel.exec_command(cmd)
        elif (command=="matrix"):
            cmd="conda activate py37; python /home/oomori/stress_gpu/randMatrix.py %s %s & \n"\
            %(exe_t, 2500)
            ssh_channel.exec_command(cmd)
        else:
            print ("invalid command\n")

        if (ssh_channel.recv_exit_status() != 0):
            print(ssh_channel.recv_stderr(1024))

    except paramiko.SSHException as e:
        print (e)

class Server:
    previous_start=0
    start_time=0
    def __init__(self, i):
        self.processing=[]
        self.id=i
        self.sum_ctime=0
        self.Tex = [T_ex_0_AVERAGE]
        self.Tcs = [[T1_0_AVERAGE], [T2_0_AVERAGE], [Tg_0_AVERAGE]]
        self.Tc_ests = [[T1_0_AVERAGE], [T2_0_AVERAGE], [Tg_0_AVERAGE]]
        self.Teff = [0]
        self.last_processed = None
    def sync(self, current_time):
        currlist = [x for x in self.processing if x.finish > current_time]
        last_procs = [x for x in self.processing if x.finish <= current_time]
        if (last_procs): self.last_processed = sorted(last_procs, key=lambda x: x.finish)[-1]
        self.processing = currlist
        return
    def virtualSync(self, next_time):
        last_processed = None
        virtuallist = [x for x in self.processing if x.finish > next_time]
        last_procs = [x for x in self.processing if x.finish <= next_time]
        if (last_procs): last_processed = sorted(last_procs, key=lambda x: x.finish)[-1]
        cload = 0
        gload = "none"
        for i in virtuallist:
            cload = cload + i.cload
            if (i.gload != "none"): 
                gload = i.gload
                break
        return virtuallist, last_processed, cload, gload
    def getLoad(self):
        cload = 0
        gload = "none"
        for i in self.processing:
            cload = cload + i.cload
            if (i.gload != "none"): 
                gload = i.gload
                break
        return cload, gload
    
    def oak_model(self, qcs, Tamb_0, T_ex_0, Tc_0s, Tc_est_0s, T_eff_0):
        qeff=[qeff_AVERAGE]*(len(qcs[0]))

        T_amb = [Tamb_0]*(len(qcs[0]))
        time = range(len(qcs[0]))

        qc_est_a = []#assumed qc
        qc_est = [[] for i in range(len(qcs))]#averaged qc_est

        for i in range(len(qcs)):
            qc_est_a.append(np.array(qcs[i])*R[i] - D[i])
            qc_est[i] = []

        t = []
        dt = []
        for i in range (len(qcs[0])):
            t.append(float(time[i] - time[0])/(np.timedelta64(1, 's')).astype(float))
            if (i != 0):
                dt.append((t[i] - t[i-1]))
        for i in range(len(qcs)):
            for j in range(len(qcs[0])): 
                if (qc_est_a[i][j] < 0): 
                    qc_est_a[i][j]=0
                if (j < 5):
                    qc_est[i].append(np.sum(qc_est_a[i][:j+1])/(j+1))
                else:
                    qc_est[i].append(np.sum(qc_est_a[i][j-4:j+1])/5)

        if (Tc_0s[0] < b_temp):
            Hc[0] = HcA
        else:
            Hc[0] = HcB
        if (Tc_0s[1] < b_temp):
            Hc[1] = HcA
        else:
            Hc[1] = HcB

        T_eff = []

        if (T_eff_0 == 0):
            T_eff.append((Cair/H)*T_ex_0 + \
                     (1-((0+lamda1+lamda2)-(lamda1+lamda2)*(Hc[1]/Cair)*(Hc[2]/Cair))*(Hc[0]/Cair) - ((lamda1+lamda2)-lamda2*Hc[2]/Cair)*Hc[1]/Cair - lamda2*Hc[2]/Cair - Cair/H) * T_amb[0] \
                     + (Hc[0]/Cair) * ((0+lamda1+lamda2)-(lamda1+lamda2)*Hc[1]/Cair+lamda2*(Hc[1]/Cair)*(Hc[2]/Cair)) * Tc_0s[0] \
                     + (Hc[1]/Cair) * ((lamda1+lamda2)-lamda2*Hc[2]/Cair) * Tc_0s[1]\
                     + lamda2*Hc[2]/Cair * Tc_0s[2])
        else:
            T_eff.append(T_eff_0)
        
        T_c = [[] for i in range(len(qcs))]
        T_c_est =[[] for i in range(len(qcs))]
        Tc_0_a = [T1_0_AVERAGE, T2_0_AVERAGE, Tg_0_AVERAGE]
        for i in range(len(Tc_0s)):
            T_c[i] = []
            T_c[i].append(Tc_0s[i])
            T_c_est[i] = []
            T_c_est[i].append(Tc_est_0s[i])

        for j in range(1, len(qcs[0])):
            for i in range(len(Tc_0s)):
                #calculate actual hardware temperature
                T_c_j = (1-(dt[j-1]*Hc[i]/Cc[i]))*T_c[i][j-1] + \
                ((Hc[i]/Cc[i])*Tc_0_a[i]+ (1/Cc[i])*qcs[i][j-1])*dt[j-1]
                
                T_c[i].append(T_c_j)

            if (T_c[0][j] < b_temp and T_c[0][j-1] <= T_c[0][j]): 
                Hc[0] = HcA
            else :
                Hc[0] = HcB
            if (T_c[1][j] < b_temp and T_c[1][j-1] <= T_c[1][j]):
                Hc[1] = HcA
            else :
                Hc[1] = HcB

            for i in range(len(Tc_0s)):
                #calculate assumed hardware temperature
                T_c_est_j = (1-(dt[j-1]*Hc[i]/Cc[i]))*T_c_est[i][j-1] + \
                ((Hc[i]/Cc[i])*Tc_0_a[i] + (1/Cc[i])*qc_est[i][j-1])*dt[j-1]

                T_c_est[i].append(T_c_est_j)

            T_eff_j = (1-(dt[j-1]*H/Ceff))*T_eff[j-1] + \
            ((H/Ceff) * (1-((0+lamda1+lamda2) - (lamda1+lamda2)*Hc[1]/Cair + lamda2*(Hc[1]/Cair)*(Hc[2]/Cair))*Hc[0]/Cair - ((lamda1+lamda2)-lamda2*Hc[2]/Cair)*Hc[1]/Cair - lamda2*Hc[2]/Cair)*T_amb[j-1]+\
            (H/Ceff) * ((0+lamda1+lamda2)-(lamda1+lamda2)*(Hc[1]/Cair)+lamda2*(Hc[1]/Cair)*(Hc[2]/Cair))*(Hc[0]/Cair)*T_c_est[0][j-1]+\
            (H/Ceff) * ((lamda1+lamda2)-lamda2*Hc[2]/Cair)*Hc[1]/Cair * T_c_est[1][j-1]+\
            (H/Ceff) * lamda2 * (Hc[2]/Cair) * T_c_est[2][j-1]+\
            (1/Ceff) * qeff[j-1])*dt[j-1]

            T_eff.append(T_eff_j)

        T_ex_predict = []

        T_ex_predict.append(T_ex_0)
        if (Tc_0s[0] < b_temp):
            Hc[0] = HcA
        else:
            Hc[0] = HcB
        if (Tc_0s[1] < b_temp):
            Hc[1] = HcA
        else:
            Hc[1] = HcB

        for j in range(1, len(qcs[0])):
            if (T_c[0][j] < b_temp and T_c[0][j-1] <= T_c[0][j]): 
                Hc[0] = HcA
            else :
                Hc[0] = HcB
            if (T_c[1][j] < b_temp and T_c[1][j-1] <= T_c[1][j]): 
                Hc[1] = HcA
            else :
                Hc[1] = HcB

            parameters = np.array([[H/Cair, 1, -(H/Cair)*(1-((0+lamda1+lamda2) - (lamda1+lamda2)*Hc[1]/Cair + lamda2*(Hc[1]/Cair)*(Hc[2]/Cair))*(Hc[0]/Cair) - ((lamda1+lamda2)-lamda2*Hc[2]/Cair)*Hc[1]/Cair - lamda2*Hc[2]/Cair), -1/Cair, 1/Cair, (Hc[0]/Cair)-(H/Cair)*(Hc[0]/Cair)*((0+lamda1+lamda2)-(lamda1+lamda2)*Hc[1]/Cair+lamda2*(Hc[1]/Cair)*(Hc[2]/Cair)), (Hc[1]/Cair)-(H/Cair)*(Hc[1]/Cair)*((lamda1+lamda2)-lamda2*Hc[2]/Cair), (Hc[2]/Cair)-(H/Cair)*(Hc[2]/Cair)*lamda2, -Hc[0]/Cair, -Hc[1]/Cair, -Hc[2]/Cair]])

            var = np.array([[T_eff[j-1]], [T_amb[j]], [T_amb[j-1]], [qeff[j-1]+qc_est[0][j-1]+qc_est[1][j-1]+qc_est[2][j-1]], [qeff[j]+qc_est[0][j]+qc_est[1][j]+qc_est[2][j]], [T_c_est[0][j-1]], [T_c_est[1][j-1]], [T_c_est[2][j-1]], [T1_0_AVERAGE], [T2_0_AVERAGE], [Tg_0_AVERAGE]])
            T_ex_predict.append(parameters.dot(var)[0,0])

        return T_ex_predict, T_c, T_c_est, T_eff
    
    def predict_online(self, next_req, predict_interval = 1):        
        return
    
    def predict_static(self, next_req, next_finish_time = 0):
        end_sec=[0]
        before_end = 0#current request start this sec before the pre finish
        processingList = self.processing#if predict determined future temperature
        if (next_finish_time != 0):#needs to predict possible future temperature
            virtualProcessing, virtual_last_processed, virtual_cload, virtual_gload = self.virtualSync(next_finish_time)
            processingList = virtualProcessing

        for processing_req in processingList:#if processing others
            pres_processing_time = processing_req.finish - next_req.server_arrival
            if (pres_processing_time > next_req.ctime):#if previous req is longer
                pres_processing_time = next_req.ctime
            end_sec.append(pres_processing_time)
            if (before_end < (processing_req.finish - next_req.server_arrival)): #result in the last start req
                before_end = processing_req.finish - next_req.server_arrival
            if (before_end >= len(self.Tex)): 
                before_end = len(self.Tex)-1
        next_req_end = next_req.ctime
        end_sec.append(next_req_end)
        last_end_sec = sorted(end_sec, reverse=True)

        qc1 = [0]*next_req.ctime
        qc2 = [0]*next_req.ctime
        qg = [0]*next_req.ctime
        i = 0
        while (i < len(end_sec)-1):           
            last_end_req = [x for x in processingList \
                            if x.finish-next_req.server_arrival >= last_end_sec[i]]
            sec_from = 0
            sec_to = 0
            loads = []
            last_end_req.insert(0, next_req)
            for req in last_end_req:
                if (i > 0): 
                    sec_from = last_end_sec[i+1]
                else: sec_from = 0
                sec_to = last_end_sec[i]
                if (req.cload != 0):
                    loads.append(req.cload)
                if (req.gload != "none"):
                    loads.append(req.gload)
            loads_set = set(loads)
            
            if (loads_set == set([50])):
                if (loads == [50]):
                    qc1[sec_from:sec_to] = [C_LOAD50_POWER]*(sec_to-sec_from)
                else:
                    qc1[sec_from:sec_to] = [C_LOAD50_50_POWER]*(sec_to-sec_from)
            elif (loads_set == set([100])):
                qc1[sec_from:sec_to] = [C_LOAD100_POWER]*(sec_to-sec_from)
            elif (loads_set == set(["matrix"])):
                qg[sec_from:sec_to] = [G_MATRIX_POWER]*(sec_to-sec_from)
                qc2[sec_from:sec_to] = [C2_MATRIX_POWER]*(sec_to-sec_from)
            elif (loads_set == set(["burn"])):
                qg[sec_from:sec_to] = [G_BURN_POWER]*(sec_to-sec_from)
                qc2[sec_from:sec_to] = [C2_BURN_POWER]*(sec_to-sec_from)
            elif (loads_set == set([50, "matrix"])):
                qc1[sec_from:sec_to] = [C_LOAD50_POWER]*(sec_to-sec_from)
                qc2[sec_from:sec_to] = [C2_MATRIX_POWER]*(sec_to-sec_from)
                qg[sec_from:sec_to] = [G_MATRIX_POWER]*(sec_to-sec_from)
            elif (loads_set == set([100, "matrix"])):
                qc1[sec_from:sec_to] = [C_LOAD100_POWER]*(sec_to-sec_from)
                qc2[sec_from:sec_to] = [C2_MATRIX_POWER]*(sec_to-sec_from)
                qg[sec_from:sec_to] = [G_MATRIX_POWER]*(sec_to-sec_from)
            elif (loads_set == set([50, "burn"])): 
                qc1[sec_from:sec_to] = [C_LOAD50_POWER]*(sec_to-sec_from)
                qc2[sec_from:sec_to] = [C2_BURN_POWER]*(sec_to-sec_from)
                qg[sec_from:sec_to] = [G_BURN_POWER]*(sec_to-sec_from)
            elif (loads_set == set([100, "burn"])):
                qc1[sec_from:sec_to] = [C_LOAD100_POWER]*(sec_to-sec_from)
                qc2[sec_from:sec_to] = [C2_BURN_POWER]*(sec_to-sec_from)
                qg[sec_from:sec_to] = [G_BURN_POWER]*(sec_to-sec_from)

            if (len(last_end_req)>2): #if end at the same time
                i = i+len(last_end_req)-1 #skip the next loop
            i = i+1

        Tex_before_start = []
        Tcs_before_start = [[], [], []]
        Tc_ests_before_start = [[], [], []]
        Teff_before_start = []
        
        last_Tex = self.Tex[-before_end-1]
        last_Tcs = np.array(self.Tcs)[:, -before_end-1]
        last_Tc_ests = np.array(self.Tc_ests)[:, -before_end-1]
        last_Teff = self.Teff[-before_end-1]

        last_processed_req = self.last_processed

        if (next_finish_time != 0):
            last_processed_req = virtual_last_processed
        
        if ((not processingList) and (last_processed_req is not None)):
            idletime = next_req.server_arrival-last_processed_req.finish
            if (idletime > 0):#falling
                idle_qc1 = [0]*idletime
                idle_qc2 = [0]*idletime
                idle_qg = [0]*idletime
                Tex_before_start, Tcs_before_start, Tc_ests_before_start, Teff_before_start = self.oak_model([idle_qc1, idle_qc2, idle_qg], \
                                                                  Tamb_0 = T_amb_AVERAGE, \
                                                                  T_ex_0 = last_Tex, \
                                                                  Tc_0s=last_Tcs, \
                                                                  Tc_est_0s=last_Tc_ests, \
                                                                  T_eff_0 = last_Teff)
                last_Tex = Tex_before_start[-1]
                last_Tcs = np.array(Tcs_before_start)[:, -1]
                last_Tc_ests = np.array(Tc_ests_before_start)[:, -1]
                last_Teff = Teff_before_start[-1]
                #before_end = before_end + idletime

        qc1_0 = qc1[0]
        qc2_0 = qc2[0]
        qg_0 = qg[0]    
        qc1.insert(0, qc1_0)
        qc2.insert(0, qc2_0)
        qg.insert(0, qg_0)             
                     
        #current_Tamb = get_db("Tamb")

        Tex_ing, Tcs_ing, Tc_ests_ing, Teff_ing = self.oak_model([qc1, qc2, qg], Tamb_0 = T_amb_AVERAGE, \
                                                                   T_ex_0 = last_Tex, Tc_0s = last_Tcs, Tc_est_0s = last_Tc_ests, T_eff_0 = last_Teff)
        Tex_before_start[-1:] = Tex_ing
        predict_Tex = Tex_before_start
        for i in range(len(Tcs_ing)):
            Tcs_before_start[i][-1:] = Tcs_ing[i]
        predict_Tcs = Tcs_before_start
        for i in range(len(Tc_ests_ing)):
            Tc_ests_before_start[i][-1:] = Tc_ests_ing[i]
        predict_Tc_ests = Tc_ests_before_start
        Teff_before_start[-1:] = Teff_ing
        predict_Teff = Teff_before_start

        return predict_Tex, predict_Tcs, predict_Tc_ests, predict_Teff, before_end
        
    def allocate(self, next_req, count):
        if (next_req.id == 0):
            Server.previous_req = next_req
        next_req.server_id = self.id
        self.processing.append(next_req)
        '''
        if (self.id == 0):
            Server.previous_start = Server.start_time
            self.sum_ctime = self.sum_ctime + next_req.ctime
            while (True):
                Server.start_time = time.time()
                if ((Server.start_time-Server.previous_start) > \
                    (next_req.server_arrival-Server.previous_req.server_arrival)):
                    count = count+1
                    if (next_req.cload != 0):
                        oak_execute("cpu1", next_req.cload, next_req.ctime)
                    if (next_req.gload == "burn"):
                        oak_execute("burn", 0, next_req.ctime)
                    elif (next_req.gload == "matrix"):
                        oak_execute("matrix", 0, next_req.ctime)
                    Server.previous_req = next_req
                    break
                    '''
        return count
    
    def allocate_later(self, reqs, count):        
        Server.start_time = 0
        sorted_reqs = sorted(reqs, key=lambda x: x.server_arrival)
        for req in sorted_reqs:
            #need to sort. revise later.
            if (count == 0):
                Server.previous_req = req                                    
                process_start = datetime.datetime.now()
            if (req.server_id == self.id):
                Server.previous_start = Server.start_time
                self.sum_ctime = self.sum_ctime + req.ctime
                while (True):
                    Server.start_time = time.time()
                    if ((Server.start_time-Server.previous_start) > \
                        (req.server_arrival-Server.previous_req.server_arrival)):
                        count = count+1
                        print(datetime.datetime.now())
                        if (req.cload != 0):
                            oak_execute("cpu1", req.cload, req.ctime)
                        if (req.gload == "burn"):
                            oak_execute("burn", 0, req.ctime)
                        elif (req.gload == "matrix"):
                            oak_execute("matrix", 0, req.ctime)
                        Server.previous_req = req
                        break
        return count, process_start

import json
import urllib3
import math
from datetime import datetime, timedelta, timezone
import statistics


#do not commit
DT_TENANT = "https://abc123.live.dynatrace.com"
DT_AUTHTOKEN = "Api-Token dt0c01.ABC1234.ABC12345"


DT_GET_ALL_ENTITIES_URL = DT_TENANT + "/api/v2/entities?entitySelector=type%28%22HOST%22%29"
DT_GET_EACH_ENTITY_URL = DT_TENANT + "/api/v2/entities/"
DT_GET_METRICS_CPU_URL = DT_TENANT + "/api/v2/metrics/query?metricSelector=builtin:host.cpu.usage&from=-14d&to=now&resolution=1h"
DT_GET_METRICS_MEMORY_URL = DT_TENANT + "/api/v2/metrics/query?metricSelector=builtin:host.mem.usage&from=-14d&to=now&resolution=1h"
DT_GET_DISK_USE_URL = DT_TENANT + '/api/v2/metrics/query?metricSelector=builtin:host.disk.usedPct:filter(eq("dt.entity.host",{selected_host})):merge("dt.entity.disk"):avg:names&from=-14d&to=now&resolution=1h'
headers =  headers = {"Authorization": DT_AUTHTOKEN, "Accept": "application/json"}
csv_headers = ["*Server name", "IP addresses","*Cores", "*Memory (In MB)", "*OS name", "OS architecture","CPU utilization percentage","Memory utilization percentage"]
aws_headers = ["entity_id", "CPU.NumberOfProcessors","CPU.NumberOfCores","RAM.TotalSizeInMB","RAM.UsedSizeInMB.Avg","RAM.UsedSizeInMB.Max","CPU.UsagePct.Avg","CPU.UsagePct.Max"]
aws_tab2_headers = ["entity_id", "maxCpuUsagePctDec (%)", "avgCpuUsagePctDec (%)", "maxRamUsagePctDec (%)","avgRamUtlPctDec (%)","Uptime","Storage-Utilization %"]


#helper function: make request
def make_http_request(method, url, headers):
    http = urllib3.PoolManager()
    r = http.request(method, url, headers=headers)
    data = r.data
    return json.loads(data)


def get_all_entites():
    response = make_http_request("GET", DT_GET_ALL_ENTITIES_URL, headers)
    return(response)

def extract_entity_id_from_all(entity_arr):
    entity_id_arr = []
    for entity in entity_arr: 
        entity_id_arr.append(entity["entityId"])
    return entity_id_arr
        

def get_each_entity(entity_id):
    url = DT_GET_EACH_ENTITY_URL + entity_id
    return make_http_request("GET", url, headers)

def get_metrics_memory():
    return make_http_request("GET",DT_GET_METRICS_MEMORY_URL, headers)

def get_metrics_cpu():
    return make_http_request("GET",DT_GET_METRICS_CPU_URL, headers)

def get_max_memory_by_host_id(memory_arr):
    max_memory_map = {}
    #TODO: make sure result isn't empty
    data = memory_arr["result"][0]["data"]
    for host_data in data:
        max_memory = 0
        host_name = host_data["dimensions"][0]
        values = host_data["values"]
        for value in values:
            if value is not None:
                if value > max_memory:
                    max_memory= value
        max_memory_map[host_name] = max_memory
    return max_memory_map

def get_avg_memory_by_host_id(memory_arr):
    avg_mem_map = {}
    mem_total = 0
    mem_count = 0
    #TODO: make sure result isn't empty
    data = memory_arr["result"][0]["data"]
    for host_data in data:
        mem_total = 0
        mem_count = 0
        host_name = host_data["dimensions"][0]
        values = host_data["values"]
        for value in values:
            if value is not None:
                mem_total += value
                mem_count += 1
        avg_mem = mem_total/mem_count        
        avg_mem_map[host_name] = avg_mem
    return avg_mem_map

def get_max_cpu_by_host_id(cpu_arr):
    max_cpu_map = {}
    #TODO: make sure result isn't empty
    data = cpu_arr["result"][0]["data"]
    for host_data in data:
        max_cpu = 0
        host_name = host_data["dimensions"][0]
        values = host_data["values"]
        for value in values:
            if value is not None:
                if value > max_cpu:
                    max_cpu = value
        max_cpu_map[host_name] = max_cpu
    return max_cpu_map

def get_avg_cpu_by_host_id(cpu_arr):
    avg_cpu_map = {}
    cpu_total = 0
    cpu_count = 0
    #TODO: make sure result isn't empty
    data = cpu_arr["result"][0]["data"]
    for host_data in data:
        cpu_total = 0
        cpu_count = 0
        host_name = host_data["dimensions"][0]
        values = host_data["values"]
        for value in values:
            if value is not None:
                cpu_total += value
                cpu_count += 1
        avg_cpu = cpu_total/cpu_count        
        avg_cpu_map[host_name] = avg_cpu
    return avg_cpu_map

def get_storage_use_perc(entity_id_arr):
    storage_use_perc_map = {}
    for entity_id in entity_id_arr:
        host_spec_url = DT_GET_DISK_USE_URL.format(selected_host =entity_id)
        response = make_http_request("GET", host_spec_url, headers)
        if response["result"][0]['dataPointCountRatio'] != 0:
            data = response['result'][0]['data'][0]['values']
            data_san = [x for x in data if x is not None]
            storage_use_perc_map[entity_id] = statistics.mean(data_san)
        else:
            storage_use_perc_map[entity_id] = 0
    return storage_use_perc_map

#time CPU is above 5%/total uptime     
def caluclate_uptime(cpu_arr):
    uptime_cpu_map = {}
    #TODO: make sure result isn't empty
    data = cpu_arr["result"][0]["data"]
    for host_data in data:
        cpu_active = 0
        cpu_count = 0
        host_name = host_data["dimensions"][0]
        values = host_data["values"]
        for value in values:
            if value is not None:
                cpu_count += 1
                if value > 5:
                    cpu_active += value
               
        uptime = cpu_active/cpu_count        
        uptime_cpu_map[host_name] = uptime
    return uptime_cpu_map

#TODO: handle next page key on all requests
def gather_dyantrace_data():
    print("fetching entity information")
    get_all_entites_response = get_all_entites()
    entity_id_arr = extract_entity_id_from_all(get_all_entites_response["entities"])
    #print(entity_id_arr)

    entity_arr = []
    for entity_id in entity_id_arr:
        entity_arr.append(get_each_entity(entity_id))
    
    #print(entity_arr)
    
    print("fetching storage use")
    storage_use_perc_map = get_storage_use_perc(entity_id_arr)
    
    print("fetching memory metrics")
    memory_metrics = get_metrics_memory()
    print("calculating memory metrics")
    memory_max_map = get_max_memory_by_host_id(memory_metrics)
    memory_avg_map = get_avg_memory_by_host_id(memory_metrics)
    print("fetching cpu metrics")
    cpu_metrics = get_metrics_cpu()
    print("calculating cpu metrics")
    cpu_max_map = get_max_cpu_by_host_id(cpu_metrics)
    cpu_avg_map = get_avg_cpu_by_host_id(cpu_metrics)
    cpu_uptime =  caluclate_uptime(cpu_metrics)

    print("formatting data")
    csv = format_dynatrace_data(entity_arr, memory_max_map, cpu_max_map, memory_avg_map, cpu_avg_map, storage_use_perc_map, cpu_uptime)
    csv_string = format_csv_to_string(csv, "tab1.csv")
    return csv_string
    

def format_dynatrace_data(entity_arr, memory_max_map, cpu_max_map, memory_avg_map, cpu_avg_map, storage_use_perc_map, cpu_uptime):
    csv = []
    master_ip_list = []
    csv_2 = []
    csv_2.append(aws_tab2_headers)
    csv.append(aws_headers)
    for entity in entity_arr:
        properties = entity['properties']

        entity_id = entity['entityId']
        #CPU.NumberOfProcessors
        cpu_processors = "N/A"
        #"CPU.NumberOfCores"
        if 'cpuCores' in properties:
            cpu_cores = properties['cpuCores']
        else:
            cpu_cores = 0
        #"RAM.TotalSizeInMB"
        if 'physicalMemory' in properties:
            total_mem = math.floor(float(properties['physicalMemory'])/1000000)
        else:
            total_mem = 0
        #"RAM.UsedSizeInMB.Avg"
        avg_mem = 0
        if entity_id in memory_avg_map.keys():
            avg_mem = math.floor(total_mem * (memory_avg_map[entity_id]/100))
        #"RAM.UsedSizeInMB.Max"
        max_memory = 0
        if entity_id in memory_max_map.keys():
            max_memory = math.floor(total_mem* (memory_max_map[entity['entityId']]/100))
        #"CPU.UsagePct.Avg"
        avg_cpu = 0
        if entity_id in cpu_avg_map.keys():
            avg_cpu = round(cpu_avg_map[entity_id],2)
        #"CPU.UsagePct.Max"
        max_cpu = 0
        if entity_id in cpu_max_map.keys():
            max_cpu = round(cpu_max_map[entity_id],2)
        row = [entity_id,cpu_processors,cpu_cores,total_mem,avg_mem,max_memory,avg_cpu,max_cpu]
        csv.append(row)

        #"maxCpuUsagePctDec (%)"
        #"avgCpuUsagePctDec (%)"
        #"maxRamUsagePctDec (%)"
        percent_mem_max = 0
        if entity_id in memory_max_map.keys():
            percent_mem_max = round(memory_avg_map[entity_id],2)
        #"avgRamUtlPctDec (%)"
        percent_avg_mem = 0
        if entity_id in memory_avg_map.keys():
            percent_avg_mem = round(memory_avg_map[entity_id],2)
        #"Uptime"
        uptime = 0
        if entity_id in cpu_uptime.keys():
            uptime = round(cpu_uptime[entity_id], 2)
        #"Storage-Utilization %"
        storage_utilization = 0
        if entity_id in storage_use_perc_map.keys():
            storage_utilization = round(storage_use_perc_map[entity_id],2)

        row = [entity_id,max_cpu,avg_cpu,percent_mem_max,percent_avg_mem, uptime, storage_utilization]
        csv_2.append(row)

        format_csv_to_string(csv_2, "tab2.csv")
        print("done")
    return csv

def format_csv_to_string(csv, document_name):
    csv_string = ""
    for row in csv:
        row_string = ",".join(str(x) for x in row) + "\n"
        csv_string = csv_string + row_string


    f = open(document_name, "w")
    f.write(csv_string)
    f.close()
    return csv_string

gather_dyantrace_data()


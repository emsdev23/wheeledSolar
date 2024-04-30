import requests
import json
import datetime,time
from time import sleep
import mysql.connector
from datetime import datetime
import tzlocal
import logging

import Class_Definition

#Database_Connection
conn = None
conn = mysql.connector.connect(host='43.205.196.66',database='EMS',user='emsroot',password='22@teneT',port=3307)
cursor = conn.cursor()

#Log files
logging.basicConfig(filename="logfile.log",  
               format='%(asctime)s %(message)s',  
               filemode='w')
               
#Creating an object of the logging  
logger=logging.getLogger()  

#Setting the threshold of logger to DEBUG  
logger.setLevel(logging.DEBUG)  


def Connection() :
       
     endTime =  int(time.mktime(datetime.now().timetuple()) * 1000 + datetime.now().microsecond/1000)   
     startTime = endTime - (5*60000)  #1min = 60000 milliseconds   
     print(endTime,startTime)
     frequency = 1
 
     url = Class_Definition.Config.config['connection']['url']
     url = url.format(startTime,endTime,frequency) 
     
     status= ""
     response_timestamp = ""
     
     try : 
          response_API = requests.get(url, headers = {"X-API-KEY": Class_Definition.Config.config['connection']['X-API-KEY']})
          if(response_API):
            response_timestamp = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            status = "Success"
            
          else:
            status = "failed"
            
          data = response_API.text
          response = json.loads(data)  #json data
          #print(response) 
          return url,response,response_timestamp,status;   #returns a tuple of values
          
     except:
          logger.error("Connection error")  
          

def ReqRes_Time() :
     url,response,response_timestamp,status = Connection ()
     print(response)
     request_timestamp = response['data']['requesttimestamp']/1000
     local_timezone = tzlocal.get_localzone() # get pytz timezone
     request_timestamp_local_time = datetime.fromtimestamp(request_timestamp, local_timezone).strftime("%Y-%m-%d %H:%M:%S") #from unix to localtime
 
     
     insert_query = """ INSERT INTO EMSResTimestamp (reqtimestamp,restimestamp,reqresrequest,reqresresponse,reqresstatus) VALUES (%s,%s,%s,%s,%s)"""
     record_to_insert = (request_timestamp_local_time,response_timestamp,url,str(response),status)
     cursor.execute(insert_query, record_to_insert)
      
     conn.commit() 
     
def Types() :
     url,response,response_timestamp,status = Connection ()
     #WMS_TYPES
     wmsMeta_type_id = response['data']['meta']['wms'][0]['wmstypeid']
     wmsMeta_count = response['data']['meta']['wms'][0]['wmscount']
     wmsMeta_type = response['data']['meta']['wms'][0]['wmstype']
     wmsMeta_make = response['data']['meta']['wms'][0]['wmsmake']
     wmsMeta_model = response['data']['meta']['wms'][0]['wmsmodel']
     
     cursor.execute("""SELECT wmstypeid FROM EMSWMSTypes""")
     type_id_wms = cursor.fetchall()

     #type_id_wms = None

     if(type_id_wms == []) :     #adding first type id value
          insert_query = """ INSERT INTO EMSWMSTypes (wmstypeid,wmscount,wmstype,wmsmake,wmsmodel) VALUES (%s,%s,%s,%s,%s)"""
          record_to_insert = (wmsMeta_type_id,wmsMeta_count,wmsMeta_type,wmsMeta_make,wmsMeta_model)
          cursor.execute(insert_query, record_to_insert)
          conn.commit()

          print("WMS Type inserted")
     
     for i in type_id_wms:
       if(wmsMeta_type_id not in i) :  #if wmsMeta_type_id is not in db then add 
         
          insert_query = """ INSERT INTO EMSWMSTypes (wmstypeid,wmscount,wmstype,wmsmake,wmsmodel) VALUES (%s,%s,%s,%s,%s)"""
          record_to_insert = (wmsMeta_type_id,wmsMeta_count,wmsMeta_type,wmsMeta_make,wmsMeta_model)
          cursor.execute(insert_query, record_to_insert)
          conn.commit()

          print("WMS Type inserted")
     
     #INVERTER_TYPES
     inverterMeta_type_id = response['data']['meta']['inverters'][0]['invertertypeid']
     inverterMeta_count = response['data']['meta']['inverters'][0]['invertercount']
     inverterMeta_string_inverterCount = response['data']['meta']['inverters'][0]['stringinvertercount']
     inverterMeta_type = response['data']['meta']['inverters'][0]['invertertype']
     inverterMeta_inverter_make = response['data']['meta']['inverters'][0]['invertermake']
     inverterMeta_inverter_model = response['data']['meta']['inverters'][0]['invertermodel']
     inverterMeta_capacity = response['data']['meta']['inverters'][0]['plant capacity']
     inverterMeta_panelMake = response['data']['meta']['inverters'][0]['panelmake']
     inverterMeta_panelEfficiency = response['data']['meta']['inverters'][0]['panelefficency']

     inverterMeta_type_id1 = response['data']['meta']['inverters'][1]['invertertypeid']
     inverterMeta_count1 = response['data']['meta']['inverters'][1]['invertercount']
     inverterMeta_string_inverterCount1 = response['data']['meta']['inverters'][1]['stringinvertercount']
     inverterMeta_type1 = response['data']['meta']['inverters'][1]['invertertype']
     inverterMeta_inverter_make1 = response['data']['meta']['inverters'][1]['invertermake']
     inverterMeta_inverter_model1 = response['data']['meta']['inverters'][1]['invertermodel']
     inverterMeta_capacity1 = response['data']['meta']['inverters'][1]['plant capacity']
     inverterMeta_panelMake1 = response['data']['meta']['inverters'][1]['panelmake']
     inverterMeta_panelEfficiency1 = response['data']['meta']['inverters'][1]['panelefficency']
     
     cursor.execute("select invertertypeid from EMSInverterTypes")
     type_id_inverter = cursor.fetchall()
     #type_id_inverter = None
     if(type_id_inverter == []) :
         insert_query = """ INSERT INTO EMSInverterTypes (invertertypeid,invertercount,inverterstringinvertercount,invertertype,invertermake,invertermodel,invertercapacity,inverterpanelmake,inverterpanelefficency) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
         record_to_insert = (inverterMeta_type_id,inverterMeta_count,inverterMeta_string_inverterCount,inverterMeta_type,inverterMeta_inverter_make,inverterMeta_inverter_model,inverterMeta_capacity,inverterMeta_panelMake,inverterMeta_panelEfficiency)
         cursor.execute(insert_query, record_to_insert)
         conn.commit()
         print("Inverter Type inserted")

         record_to_insert = (inverterMeta_type_id1,inverterMeta_count1,inverterMeta_string_inverterCount1,inverterMeta_type1,inverterMeta_inverter_make1,inverterMeta_inverter_model1,inverterMeta_capacity1,inverterMeta_panelMake1,inverterMeta_panelEfficiency1)
         cursor.execute(insert_query, record_to_insert)
         conn.commit()
         print("Inverter Type inserted")
         
     
         if(inverterMeta_type_id not in type_id_inverter) :
            insert_query = """ INSERT INTO EMSInverterTypes (invertertypeid,invertercount,inverterstringinvertercount,invertertype,invertermake,invertermodel,invertercapacity,inverterpanelmake,inverterpanelefficency) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            record_to_insert = (inverterMeta_type_id,inverterMeta_count,inverterMeta_string_inverterCount,inverterMeta_type,inverterMeta_inverter_make,inverterMeta_inverter_model,inverterMeta_capacity,inverterMeta_panelMake,inverterMeta_panelEfficiency)
            cursor.execute(insert_query, record_to_insert)
            conn.commit()
            print("Inverter Type inserted")
    
    
         if(inverterMeta_type_id1 not in type_id_inverter) :
            insert_query = """ INSERT INTO EMSInverterTypes (invertertypeid,invertercount,inverterstringinvertercount,invertertype,invertermake,invertermodel,invertercapacity,inverterpanelmake,inverterpanelefficency) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            record_to_insert = (inverterMeta_type_id1,inverterMeta_count1,inverterMeta_string_inverterCount1,inverterMeta_type1,inverterMeta_inverter_make1,inverterMeta_inverter_model1,inverterMeta_capacity1,inverterMeta_panelMake1,inverterMeta_panelEfficiency1)
            cursor.execute(insert_query, record_to_insert)
            conn.commit()
            print("Inverter Type inserted")
     
     #METER_TYPES
     meterMeta_type_id = response['data']['meta']['meters'][0]['metertypeid']
     meterMeta_count = response['data']['meta']['meters'][0]['metercount']
     meterMeta_type = response['data']['meta']['meters'][0]['metertype']
     meterMeta_make = response['data']['meta']['meters'][0]['metermake']
     meterMeta_model = response['data']['meta']['meters'][0]['metermodel']
     
     cursor.execute("select metertypeid from EMSMeterTypes")
     type_id_meter = cursor.fetchall()
     #type_id_meter = None
     if(type_id_meter==[]):
          insert_query = """ INSERT INTO EMSMeterTypes (metertypeid,metercount,metertype,metermake,metermodel) VALUES (%s,%s,%s,%s,%s)"""
          record_to_insert = (meterMeta_type_id,meterMeta_count,meterMeta_type,meterMeta_make,meterMeta_model)
          cursor.execute(insert_query, record_to_insert)
          conn.commit()
          print("Meter Type inserted")
          
     for i in type_id_meter :
          if(meterMeta_type_id not in  i) :
     
             insert_query = """ INSERT INTO EMSMeterTypes (metertypeid,metercount,metertype,metermake,metermodel) VALUES (%s,%s,%s,%s,%s)"""
             record_to_insert = (meterMeta_type_id,meterMeta_count,meterMeta_type,meterMeta_make,meterMeta_model)
             cursor.execute(insert_query, record_to_insert)
             conn.commit()
             print("Meter Type inserted")
             
def Instances () :
     url,response,response_timestamp,status = Connection ()
     
     #WMS_INSTANCES
     if (response['data']['data'][0]['wms'] != []):
        print("WMS Data")
        try:
             wms_inst_type_id  = response['data']['meta']['wms'][0]['wmstypeid']
             wms_inst_name =  response['data']['data'][0]['wms'][0]['name'] 
             wms_inst_device_id = response['data']['data'][0]['wms'][0]['deviceid']
             
             cursor.execute("select wmsdeviceid from  EMSWMSInstances")
             device_id_wms = cursor.fetchall()
             device_id_wms = None
             if(device_id_wms == []) :
                  insert_query = """ INSERT INTO EMSWMSInstances (wmstypeid,wmsname,wmsdeviceid) VALUES (%s,%s,%s)"""
                  record_to_insert = (wms_inst_type_id,wms_inst_name,wms_inst_device_id)
                  cursor.execute(insert_query, record_to_insert)
                  conn.commit()
                  print("WMS Instances inserted")
                  
             for i in device_id_wms:    
                  if(wms_inst_device_id not in  i) :
             
                     insert_query = """ INSERT INTO EMSWMSInstances (wmstypeid,wmsname,wmsdeviceid) VALUES (%s,%s,%s)"""
                     record_to_insert = (wms_inst_type_id,wms_inst_name,wms_inst_device_id)
                     cursor.execute(insert_query, record_to_insert)
                     conn.commit()
                     print("WMS Instances inserted")
                     
        except Exception as ex:
             logger.error(ex)
               
     else :
        print("wms data missing")
        logger.error("wms data missing")
               
               
     #INVERTER_INSTANCES
     if (response['data']['data'][0]['inverter'] != []):
        print("Inverter1 Data")
        
        inverterMeta_string_inverterCount = response['data']['meta']['inverters'][0]['stringinvertercount']
        inverter_inst_type_id = response['data']['meta']['inverters'][0]['invertertypeid']
        for i in range(inverterMeta_string_inverterCount) :
            inverter_inst_name = response['data']['data'][0]['inverter'][i]['name']
            inverter_inst_device_id = response['data']['data'][0]['inverter'][i]['deviceid']
            
            cursor.execute("select inverterdeviceid from  EMSInverterInstances")
            device_id_inverter = cursor.fetchall()
            #device_id_inverter = None
            if(device_id_inverter == []) :
               insert_query = """ INSERT INTO EMSInverterInstances (invertertypeid,invertername,inverterdeviceid) VALUES (%s,%s,%s)"""
               record_to_insert = (inverter_inst_type_id,inverter_inst_name,inverter_inst_device_id)
               cursor.execute(insert_query, record_to_insert)
               conn.commit()
               print("Inverter Instances inserted")
        
        
            if(inverter_inst_device_id not in device_id_inverter[i]) :
               insert_query = """ INSERT INTO EMSInverterInstances (invertertypeid,invertername,inverterdeviceid) VALUES (%s,%s,%s)"""
               record_to_insert = (inverter_inst_type_id,inverter_inst_name,inverter_inst_device_id)
               cursor.execute(insert_query, record_to_insert)
               conn.commit()
               print("Inverted Instances inserted")
               
     else:
         print("inverter 1 data missing")
         logger.error("inverter 1 data missing")

     if (response['data']['data'][0]['inverter'] != []):
        print("Inverter2 Data")
        
        inverterMeta_string_inverterCountprev = response['data']['meta']['inverters'][0]['stringinvertercount']
        inverterMeta_string_inverterCount = abs(response['data']['meta']['inverters'][1]['stringinvertercount']+inverterMeta_string_inverterCountprev)
        inverter_inst_type_id = response['data']['meta']['inverters'][1]['invertertypeid']
        for i in range(inverterMeta_string_inverterCountprev,inverterMeta_string_inverterCount) :
            inverter_inst_name = response['data']['data'][0]['inverter'][i]['name']
            inverter_inst_device_id = response['data']['data'][0]['inverter'][i]['deviceid']
            
            cursor.execute("select inverterdeviceid from  EMSInverterInstances")
            device_id_inverter = cursor.fetchall()
            #device_id_inverter = None
            if(device_id_inverter == []) :
               insert_query = """ INSERT INTO EMSInverterInstances (invertertypeid,invertername,inverterdeviceid) VALUES (%s,%s,%s)"""
               record_to_insert = (inverter_inst_type_id,inverter_inst_name,inverter_inst_device_id)
               cursor.execute(insert_query, record_to_insert)
               conn.commit()  
               print("Inverter Instances inserted")
        
            if(inverter_inst_device_id not in device_id_inverter[i]) :
               insert_query = """ INSERT INTO EMSInverterInstances (invertertypeid,invertername,inverterdeviceid) VALUES (%s,%s,%s)"""
               record_to_insert = (inverter_inst_type_id,inverter_inst_name,inverter_inst_device_id)
               cursor.execute(insert_query, record_to_insert)
               conn.commit()
               print("Inverter Instances inserted")
               
     else:
         print("inverter 2 data missing")
         logger.error("inverter 2 data missing")
               
     #METER_INSTANCES
     if(response['data']['data'][0]['meter']!= []):
         print("Meter data")
         meter_inst_type_id = response['data']['meta']['meters'][0]['metertypeid']
         meter_inst_name = response['data']['data'][0]['meter'][0]['name'] 
         meter_inst_device_id = response['data']['data'][0]['meter'][0]['deviceid'] 
         
         cursor.execute("select meterdeviceid from  EMSMeterInstances")
         device_id_meter = cursor.fetchall()
         #device_id_meter = None
         if(device_id_meter == []) :
             insert_query = """ INSERT INTO EMSMeterInstances (metertypeid,metername,meterdeviceid) VALUES (%s,%s,%s)"""
             record_to_insert = (meter_inst_type_id,meter_inst_name,meter_inst_device_id)
             cursor.execute(insert_query, record_to_insert)
             conn.commit()
             print("Meter Instances inserted")

        #  for i in device_id_meter : 
        #      if(meter_inst_device_id not in i) :
         
        #         insert_query = """ INSERT INTO EMSMeterInstances (metertypeid,metername,meterdeviceid) VALUES (%s,%s,%s)"""
        #         record_to_insert = (meter_inst_type_id,meter_inst_name,meter_inst_device_id)
        #         cursor.execute(insert_query, record_to_insert)
        #         conn.commit()
        #         print("Meter Instances inserted")
                
     else :
         print("meter data missing")
         logger.error("meter data missing")
     
def Data() :
     url,response,response_timestamp,status = Connection ()
     
     cursor.execute("select reqresrecordid from  EMSResTimestamp order by reqtimestamp DESC LIMIT 1") #get the latest record id from reqres table
     reqres_recordid = cursor.fetchone()


     
     #print(reqres_recordid[0])
     for i in range(len(response['data']['data'])):
        data_timestamp = response['data']['data'][i]['timestamp']/1000
        local_timezone = tzlocal.get_localzone() # get pytz timezone
        data_timestamp_local_time = datetime.fromtimestamp(data_timestamp, local_timezone).strftime("%Y-%m-%d %H:%M:%S")  #unix to localtime
        
        unix_timestamp = response['data']['data'][i]['timestamp']
        
        #WMS_DATA
        if (response['data']['data'][i]['wms'] != []):
            try:
                 
                wms_device_id = response['data']['data'][i]['wms'][0]['deviceid']
                #wms_type_id = response['data']['data'][i]['wms'][0]['devicetypeid']
                #wms_name = response['data']['data'][i]['wms'][0]['name']
                wms_ambient_temp = response['data']['data'][i]['wms'][0]['ambienttemprature']
                wms_irradiation =  response['data']['data'][i]['wms'][0]['irradiation'] 
                wms_humidity = response['data']['data'][i]['wms'][0]['humidity'] 
                wms_wind_speed = response['data']['data'][i]['wms'][0]['windspeed'] 
                
                insert_query = """ INSERT INTO EMSWMSData (reqrestimerecordid,wmstimestamp,wmsdeviceid,wmsambienttemp,wmsirradiation,wmshumidity,wmswindspeed,WMSunixtimestamp) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
                record_to_insert = (reqres_recordid[0],data_timestamp_local_time,wms_device_id,wms_ambient_temp,wms_irradiation,wms_humidity,wms_wind_speed,unix_timestamp)
                cursor.execute(insert_query, record_to_insert)
                conn.commit()
                print("WMS Data inserted")
                
            except Exception as ex:
                logger.error(ex)
        
        #INVERTER_DATA
        if (response['data']['data'][i]['inverter'] != []): 
            inverterMeta_string_inverterCount = response['data']['meta']['inverters'][0]['stringinvertercount']
            # for j in range(inverterMeta_string_inverterCount) :
            #     inverter_device_id = response['data']['data'][i]['inverter'][j]['deviceid']
            #     #inverter_type_id = response['data']['data'][i]['inverter'][j]['devicetypeid'] 
            #     #inverter_name = response['data']['data'][i]['inverter'][j]['name'] 
            #     inverter_energy = response['data']['data'][i]['inverter'][j]['energy'] 
            #     inverter_active_power = response['data']['data'][i]['inverter'][j]['activepower']
            #     inverter_frequency = response['data']['data'][i]['inverter'][j]['frequency']
            #     inverter_reactive_power = response['data']['data'][i]['inverter'][j]['reactivepower']
            #     inverter_dc_power = response['data']['data'][i]['inverter'][j]['dcpower']
            #     inverter_temperature = response['data']['data'][i]['inverter'][j]['invertertemparture']
            #     inverter_status =  response['data']['data'][i]['inverter'][j]['status']
            #     inverter_power_setpoint = response['data']['data'][i]['inverter'][j]['powersetpoint']
            #     inverter_dc_current = response['data']['data'][i]['inverter'][j]['dccurrent']
            #     inverter_dc_voltage = response['data']['data'][i]['inverter'][j]['dcvoltage'] 
                
            #     insert_query = """ INSERT INTO EMSInverterData (reqrestimerecordid,invertertimestamp,inverterdeviceid,inverterEnergy,inverteractivepower,inverterfrequency,inverterreactivepower,inverterdcpower,invertertemperature,inverterstatus,inverterpowersetpoints,inverterdccurrent,inverterdcvoltage,inverterunixtimestamp) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            #     record_to_insert = (reqres_recordid[0],data_timestamp_local_time,inverter_device_id,inverter_energy,inverter_active_power,inverter_frequency,inverter_reactive_power,inverter_dc_power,inverter_temperature,inverter_status,inverter_power_setpoint,inverter_dc_current,inverter_dc_voltage,unix_timestamp)
            #     cursor.execute(insert_query, record_to_insert)
            #     conn.commit()
            #     print("Inverter1 Data inserted")
   
            inverterMeta_string_inverterCount1 = response['data']['meta']['inverters'][1]['stringinvertercount'] + inverterMeta_string_inverterCount
            for j in range(inverterMeta_string_inverterCount1) :
                inverter_device_id = response['data']['data'][i]['inverter'][j]['deviceid']
                #inverter_type_id = response['data']['data'][i]['inverter'][j]['devicetypeid'] 
                #inverter_name = response['data']['data'][i]['inverter'][j]['name'] 
                inverter_energy = response['data']['data'][i]['inverter'][j]['energy'] 
                inverter_active_power = response['data']['data'][i]['inverter'][j]['activepower']
                inverter_frequency = response['data']['data'][i]['inverter'][j]['frequency']
                inverter_reactive_power = response['data']['data'][i]['inverter'][j]['reactivepower']
                inverter_dc_power = response['data']['data'][i]['inverter'][j]['dcpower']
                inverter_temperature = response['data']['data'][i]['inverter'][j]['invertertemparture']
                inverter_status =  response['data']['data'][i]['inverter'][j]['status']
                inverter_power_setpoint = response['data']['data'][i]['inverter'][j]['powersetpoint']
                inverter_dc_current = response['data']['data'][i]['inverter'][j]['dccurrent']
                inverter_dc_voltage = response['data']['data'][i]['inverter'][j]['dcvoltage'] 
                
                insert_query = """ INSERT INTO EMSInverterData (reqrestimerecordid,invertertimestamp,inverterdeviceid,inverterEnergy,inverteractivepower,inverterfrequency,inverterreactivepower,inverterdcpower,invertertemperature,inverterstatus,inverterpowersetpoints,inverterdccurrent,inverterdcvoltage,inverterunixtimestamp) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                record_to_insert = (reqres_recordid[0],data_timestamp_local_time,inverter_device_id,inverter_energy,inverter_active_power,inverter_frequency,inverter_reactive_power,inverter_dc_power,inverter_temperature,inverter_status,inverter_power_setpoint,inverter_dc_current,inverter_dc_voltage,unix_timestamp)
                cursor.execute(insert_query, record_to_insert)
                conn.commit()
                print("Inverter Data inserted")
            
        #METER_DATA
        if(response['data']['data'][i]['meter']!= []):
            meterMeta_count = response['data']['meta']['meters'][0]['metercount']

            for j in range(meterMeta_count):
                meter_device_id = response['data']['data'][i]['meter'][j]['deviceid'] 
                #meter_type_id = response['data']['data'][i]['meter'][0]['devicetypeid']
                #meter_name = response['data']['data'][i]['meter'][0]['name'] 
                meter_energy = response['data']['data'][i]['meter'][j]['energy']
                meter_power = response['data']['data'][i]['meter'][j]['power']
            
                insert_query = """ INSERT INTO EMSMeterData (reqrestimerecordid,metertimestamp,meterdeviceid,meterenergy,meterpower,meterunixtimestamp) VALUES (%s,%s,%s,%s,%s,%s)"""
                record_to_insert = (reqres_recordid[0],data_timestamp_local_time,meter_device_id,meter_energy,meter_power,unix_timestamp)
                cursor.execute(insert_query, record_to_insert)
                conn.commit()
                print("Meter Data inserted")
     

def Main():
     
     while(1):
        try:
            Connection ()
            ReqRes_Time()
            Types()
            response_list = Connection()
            response = response_list[1]
            if(response['data']['data'] != []) :    
                Instances()
                Data ()
            
            else:
                print("data is missing")
                logger.error("Data missing")
            
        except Exception as ex:
            print(ex)
            logger.error(ex)  
            
        sleep(5*60)  
       
if __name__=="__main__":
     Main()
     
     

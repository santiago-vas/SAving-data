def saveDailyData(finalDataFrame,clientId,env):
    if len(finalDataFrame.index) > 0:
        try:
            table_ref = "lw-looker." + str(clientId) + "."+ "fleet_maintenance"
            table = clientBigQueryLookerAnalytics.get_table(table_ref)
            exist=1
        except: 
            exist=2
            
        if exist==1:
        
            queryHistory = clientBigQueryLookerAnalytics.query("""SELECT * FROM `{}.fleet_maintenance`""".format(clientId))
            resultHistory = queryHistory.result()

            oldData = resultHistory.to_dataframe()
            oldData['t']='a'
            newData=finalDataFrame.copy()
            newData['t']='z'
            data=pd.concat([oldData,newData],axis=0, sort=True)
            data.dateTime = pd.to_datetime(data.dateTime, utc=True)
            data=data.sort_values(by=['imei','dateTime','t'],ascending=True).drop_duplicates(subset=['imei'],keep='last').drop(columns='t')
            
            queryDelete = clientBigQueryLookerAnalytics.query(""" delete FROM {}.{}.fleet_maintenance where imei<>'' """.format(env,clientId)) 
            resultDelete = queryDelete.result()
            print(data.dateTime.dt.date.unique(),data.shape)            

            data.to_gbq(destination_table = "{}.fleet_maintenance".format(clientId), 
                        project_id = env, credentials = bigQueryCredentialsLookerAnalytics, if_exists = "append")
        else:
            
            finalDataFrame.to_gbq(destination_table = "{}.fleet_maintenance".format(clientId), 
                    project_id = env, credentials = bigQueryCredentialsLookerAnalytics , if_exists = "append")
    else:
        print("No hay data, no se guarda nada") 

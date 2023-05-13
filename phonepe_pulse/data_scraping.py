'''
Scrap data from pusle dataset save in csv
'''
import os
import json
import pandas as pd
pulse_main_dir_path = r"F:\work\github\pulse\data"

class extract_json_data:
    '''extract json data'''
    def __init__(self,data_base_path):
        self.data_base_path = data_base_path
    
    def extract_aggregate_data(self,json_path,country,state,data_type):
        '''
        read json file and transform it into pandas datafram
        '''
        file_data = open(json_path)
        file_dict = json.load(file_data)
        file_data.close()

        #maintain columns wise list
        name = []
        type = []
        count = []
        amount = []

        # get reuired dictionary
        # Reading json file contains a data as a main source and inside it has transactionData.
        required_dict = file_dict['data']['transactionData']
        #Extract data
        for data in required_dict:
            name.append(data['name'])
            for key, value in data['paymentInstruments'][0].items():
                if key == 'type':
                    type.append(value)
                elif key == 'count':
                    count.append(value)
                elif key == 'amount':
                    amount.append(value)
                else: print("no matching key")
        quarter = os.path.basename(json_path).split('.')[0]
        year = json_path.split('\\')[-2] 

        #return reuired dictionary
        list_of_data = []
        for i in range(len(name)):
            list_of_data.append( {
                    'data_type': data_type,
                    'country': country,
                    'state': state,
                    'year': year,
                    'quarter' : quarter,
                    "name":name[i],
                    'type' : type[i],
                    'count' : count[i],
                    'amount' : amount[i]})
        
        return list_of_data

    def extract_map_data(self,json_path,country,state,data_type):
        '''
        read json file and transform it into pandas datafram
        '''
        file_data = open(json_path)
        file_dict = json.load(file_data)
        file_data.close()

        #maintain columns wise list
        name = []
        type = []
        count = []
        amount = []

        # get reuired dictionary
        # Reading json file contains a data as a main source and inside it has transactionData.
        required_dict = file_dict['data']['hoverDataList']
        #Extract data
        for data in required_dict:
            name.append(data['name'])
            for key, value in data['metric'][0].items():
                if key == 'type':
                    type.append(value)
                elif key == 'count':
                    count.append(value)
                elif key == 'amount':
                    amount.append(value)
                else: print("no matching key")
        quarter = os.path.basename(json_path).split('.')[0]
        year = json_path.split('\\')[-2] 

        #return reuired dictionary
        list_of_data = []
        for i in range(len(name)):
            list_of_data.append( {
                    'data_type': data_type,
                    'country': country,
                    'state': state,
                    'year': year,
                    'quarter' : quarter,
                    "name":name[i],
                    'type' : type[i],
                    'count' : count[i],
                    'amount' : amount[i]})
        
        return list_of_data
    
    def extract_top_data(self,json_path,country,state,data_type):
        '''
        read json file and transform it into pandas datafram
        '''
        file_data = open(json_path)
        file_dict = json.load(file_data)
        file_data.close()

        #maintain columns wise list
        name = []
        type = []
        count = []
        amount = []

        # get reuired dictionary
        # Reading json file contains a data as a main source and inside it has transactionData.
        required_dict = file_dict['data']['states']
        #Extract data
        for data in required_dict:
            name.append(data['entityName'])
            for key, value in data['metric'][0].items():
                if key == 'type':
                    type.append(value)
                elif key == 'count':
                    count.append(value)
                elif key == 'amount':
                    amount.append(value)
                else: print("no matching key")
        quarter = os.path.basename(json_path).split('.')[0]
        year = json_path.split('\\')[-2] 

        #return reuired dictionary
        list_of_data = []
        for i in range(len(name)):
            list_of_data.append( {
                    'data_type': data_type,
                    'country': country,
                    'state': state,
                    'year': year,
                    'quarter' : quarter,
                    "name":name[i],
                    'type' : type[i],
                    'count' : count[i],
                    'amount' : amount[i]})
        
        return list_of_data

    

#define object for data extract
extract_data = extract_json_data(data_base_path = pulse_main_dir_path)
transaction_data_list = []
map_data_list = []
top_data_list = []

#walk over main dir path of pulse data
for root, dir, files in os.walk(pulse_main_dir_path):
    if not files == []:
        data_type = root.split("data")[1].split("\\")[1]
        country = root.split("country")[1].split("\\")[1]
        if root.__contains__('transaction') and root.__contains__('aggregated'):
            if root.__contains__('state'):
                state = root.split("state")[1].split("\\")[1]
            else:
                state = 'Na'
            #iterate over files to get data wrt file
            for file in files:
                file_json_path = os.path.join(root,file)
                print(file_json_path)
                extracted_data_dict = extract_data.extract_aggregate_data(
                                                        json_path= file_json_path,
                                                        country = country,
                                                        state = state,
                                                        data_type = data_type)
                transaction_data_list.extend(extracted_data_dict)
        if root.__contains__('transaction') and root.__contains__('hover'):
            if root.__contains__('state'):
                state = root.split("state")[1].split("\\")[1]
            else:
                state = 'Na'
            #iterate over files to get data wrt file
            for file in files:
                file_json_path = os.path.join(root,file)
                print(file_json_path)
                extracted_data_dict = extract_data.extract_map_data(
                                                        json_path= file_json_path,
                                                        country = country,
                                                        state = state,
                                                        data_type = data_type)
                map_data_list.extend(extracted_data_dict)

        if root.__contains__('transaction') and root.__contains__('top'):
                    if root.__contains__('state'):
                        state = root.split("state")[1].split("\\")[1]
                    else:
                        state = 'Na'
                    #iterate over files to get data wrt file
                    for file in files:
                        file_json_path = os.path.join(root,file)
                        print(file_json_path)
                        extracted_data_dict = extract_data.extract_top_data(
                                                                json_path= file_json_path,
                                                                country = country,
                                                                state = state,
                                                                data_type = data_type)
                        top_data_list.extend(extracted_data_dict)
len(transaction_data_list)
len(map_data_list)
len(top_data_list)
pd.DataFrame(transaction_data_list).to_csv("aggregated.csv")
pd.DataFrame(map_data_list).to_csv("map.csv")
pd.DataFrame(map_data_list).to_csv("top.csv")



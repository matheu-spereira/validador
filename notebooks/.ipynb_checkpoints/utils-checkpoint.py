import pandas as pd
import requests
import json
import os

class request_endpoint:

    def __init__(self, user,password,host,url_token,url_data):
        self.__user = user
        self.__password = password
        self.__host = host
        self.__url_token = url_token
        self.__url_data = url_data


    def get_response(self):

        payload_token = {
                "username": self.__user,
                "password": self.__password
            }

        headers_token = {"Content-Type": "application/json"}
        response_token = requests.request("POST", self.__url_token, json=payload_token, headers=headers_token, verify=False)
        json_token = json.loads(response_token.text)
        token = json_token['access_token']
        bearer_token = f"Bearer {token}"

        querystring_data = {
                "count":"true",
                "limit": 100,
                "offset": 0,
            }

        payload_data = {

            }
        
        headers_data = {
                "accept": "application/json",
                "Authorization": bearer_token,
                "Content-Type": "application/json"
            }

        response = requests.request("GET", self.__url_data, json=payload_data, headers=headers_data, verify=False)

        return response

    def get_endpoints(self):

        response = self.get_response()


        data = json.loads(response.text)
        endpoints_list = list(data["paths"].keys())
        filtered_list = []

        for endpoint in endpoints_list:

            if 'role' not in endpoint and 'user' not in endpoint:
                filtered_list.append(endpoint)



        return filtered_list
    
    def get_columns(self,endpoint):
            # request para pegar o token de autenticação dos endpoints
        payload_token = {
                "username": self.__user,
                "password": self.__password
            }

        headers_token = {"Content-Type": "application/json"}
        response_token = requests.request("POST", self.__url_token, json=payload_token, headers=headers_token, verify=False)
        json_token = json.loads(response_token.text)
        token = json_token['access_token']
        bearer_token = f"Bearer {token}"

        # monta os headers e query para o endpoint

        querystring_data = {
            "limit": 1000,
            "offset": 0,
            "count":",true"
        }
        
        headers_data = {
            "accept": "application/json",
            "Authorization": bearer_token,
            "Content-Type": "application/json"
        }

        url_data = f"{self.__host}{endpoint}?limit=1"
            
        response = requests.request("POST", url_data, headers=headers_data, verify=False)
        print(url_data)
        if response.status_code == 200:
            data = json.loads(response.text)
            url_data = data["next"]
            new_df = pd.DataFrame(data['results'])
            
            return new_df.columns.to_list()

        else:
            return 0
        
    def compare_endpoint_columns(self, ENDPOINTS_DR):
        result = []
        for key, endpoints in ENDPOINTS_DR.items():
            for endpoint, columns in endpoints.items():
                for column in columns:
                    for nome_dr, endpoints_dr in ENDPOINTS_DR.items():
                        if column not in endpoints_dr[endpoint]:
                            result.append(f"A coluna {column} não existe nos ENDPOINTS DE {nome_dr}.")
        return result
    
    def compare_datadictionary_columns(self, ENDPOINTS_DR, data_dictionary):
        result = []
        for key, endpoints in ENDPOINTS_DR.items():
            for endpoint, columns in endpoints.items():
                sheet = data_dictionary[endpoint]
                df = pd.read_excel("Dicionário de Dados - PRODUCAO.xlsx",sheet)[4:]
                colunas_dict = []
                for k,value in df.iterrows():
                    colunas_dict.append(value[0])
                
                for column in colunas_dict:
                    if column not in columns:
                        result.append({
                            "coluna":column,
                            "fonte": "Dicionário de dados.",
                            "endpoint": endpoint,
                            "obs" :"A coluna do dicionario não existe no endpoint.",
                            "dr": key
                        })
                        # result.append(f"A coluna {column} do dicionário não existe no endpoint {endpoint} de {key}.")
                
                for column in columns:
                    if column not in colunas_dict:
                        result.append({
                            "coluna":column,
                            "fonte": "Endpoint",
                            "endpoint": endpoint,
                            "obs" :"A coluna do endpoint não existe no dicionário.",
                            "dr": key
                        })
                        # result.append(f"A coluna {column}, endpoint {endpoint} de {key} não existe no dicionário de dados.")
        result =  pd.DataFrame(result)  
        return result
    
    def get_dados(self,endpoint):    # Conta Linhas
            # request para pegar o token de autenticação dos endpoints
        payload_token = {
                "username": self.__user,
                "password": self.__password
            }

        headers_token = {"Content-Type": "application/json"}
        response_token = requests.request("POST", self.__url_token, json=payload_token, headers=headers_token, verify=False)
        json_token = json.loads(response_token.text)
        token = json_token['access_token']
        bearer_token = f"Bearer {token}"

        # monta os headers e query para o endpoint

        querystring_data = {
            "limit": 10,
            "offset": 0,
            "count":"true"
        }
        
        headers_data = {
            "accept": "application/json",
            "Authorization": bearer_token,
            "Content-Type": "application/json"
        }

        url_data = f"{self.__host}{endpoint}?count=true&limit=10&offset=0"
            
        response = requests.request("POST", url_data, headers=headers_data, verify=False)
        print(url_data)

        if response.status_code == 200:
            data = response.json()
            if 'count' in data:
                return data['count']
            else:
                print("Não foi possível encontrar a contagem no retorno da API.")
                return None
        else:
            print(f"Falha ao obter dados do endpoint {endpoint}. Status code: {response.status_code}")
            return None
        
    def __get_token(self):
        url_token = f"{self.__host}/user/login"

        payload_token = {
            "username": self.__user,
            "password": self.__password
        }

        headers_token = {"Content-Type": "application/json"}
        response_token = requests.request("POST", url_token, json=payload_token, headers=headers_token)
        json_token = json.loads(response_token.text)
        token = json_token['access_token']
        bearer_token = f"Bearer {token}"

        return bearer_token

    def get_count(self,schema,endpoint):
        url = f"{self.__host}/{schema}/{endpoint}"

        querystring = {"count":"true",
                    "limit":"1",
                    "offset":"0"}

        payload = {}
        headers = {
            "accept": "application/json",
            "Authorization": self.__get_token(),
            "Content-Type": "application/json"
        }

        response = requests.request("POST", url, json=payload, headers=headers, params=querystring)

        data = json.loads(response.text)

        return data['count']
        
    # def filter_items(lst):

    #     # Using list comprehension to filter out items starting with '/role' or '/user'
    #     filtered_list = [item for item in lst if not item.startswith(('/role', '/user'))]
    #     return filtered_list
    
    # def strips_items(lst):
    #     filtered_list = [item.replace(['/dbo/','/prod/'], '') for item in lst]
    #     return filtered_list
    
    # def get_endpoints(self):

    #     keys = self.__get_endpoints_list()

    #     lists = list(keys)
    #     select_endpoints = self.filter_items(lists)
    #     endpoint_list = self.strips_items(select_endpoints)
        
    #     return endpoint_list

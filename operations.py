from pymongo import MongoClient
import json

#Cria a conexão com o mongodb, no banco que foi criado previamente
client = MongoClient('localhost', 27017)  

# Seta o db onde criei as collections
conn = client['case_test']

# Execute a operação de agregação
def aggregation():
   result = conn.Carros.aggregate([
      {
         "$lookup":
            {
            "from": "Montadoras",
            "localField": "Montadora",
            "foreignField": "Montadora",
            "as": "aggregation"
            }
      },
      {
         "$unwind": "$aggregation"
      },
      {
         "$project":
            {
            "_id": 1,
            "Carro": 1,
            "Cor": 1,
            "Montadora": 1,
            "Pais": "$aggregation.Pais"
            }
      }
   ])

   # Transforme o resultado em uma lista
   result_list = list(result)

   # Salve o resultado em um arquivo .js, faz um for para salvar os resultados um embaixo do outro e não tudo na mesma linha, utiliza o padrão [\n para dar a quebra de linha
   with open('result_aggregation.js', 'w', encoding='utf-8') as f:
      f.write('var result_aggregation = [\n')
      for i, item in enumerate(result_list):
         json_str = json.dumps(item, default=str, ensure_ascii=False, indent=2)
         f.write(json_str)
         if i < len(result_list) - 1:
               f.write(',')
         f.write('\n')
      f.write('];')

# Execute a operação de groupby
def groupby():
   result = conn.Carros.aggregate([
       {
      "$lookup":
         {
           "from": "Montadoras",
           "localField": "Montadora",
           "foreignField": "Montadora",
           "as": "detalhes_montadora"
         }
   },
   {
      "$unwind": "$detalhes_montadora"
   },
   {
      "$project":
         {
           "_id": 1,
           "Carro": 1,
           "Cor": 1,
           "Montadora": 1,
           "Pais": "$detalhes_montadora.Pais"
         }
   },
   {
      "$group": {
          "_id": "$Pais",
          "Carros": {
              "$push": {
                  "_id": "$_id",
                  "Carro": "$Carro",
                  "Cor": "$Cor",
                  "Montadora": "$Montadora"
              }
          }
      }
   }
   ])

   # Transforme o resultado em uma lista
   result_list = list(result)

   # Salve o resultado em um arquivo .js, faz um for para salvar os resultados um embaixo do outro e não tudo na mesma linha, utiliza o padrão [\n para dar a quebra de linha
   with open('result_groupby.js', 'w', encoding='utf-8') as f:
      f.write('var result_groupby = [\n')
      for i, item in enumerate(result_list):
         json_str = json.dumps(item, default=str, ensure_ascii=False, indent=2)
         f.write(json_str)
         if i < len(result_list) - 1:
               f.write(',')
         f.write('\n')
      f.write('];')


###ORQUESTRADOR
if __name__=='__main__':
   aggregation()
   groupby()

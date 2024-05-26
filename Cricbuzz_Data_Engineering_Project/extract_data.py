import requests
import csv
from google.cloud import storage

url = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen"

querystring = {"formatType":"odi"}

headers = {
	"X-RapidAPI-Key": "3010702ffemshac7664a26c06f7cp168231jsn34f295dbe19a",
	"X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
}


response = requests.get(url, headers=headers, params=querystring)

if response.status_code==200:
	data=response.json().get('rank',[]) #extracting the 'rank' data
	csv_filename='batsman_rankings.csv'

	if data:
		field_name=['rank','name','country','rating'] #specify required field names

		#write data to csv with only specified field names
		with open(csv_filename,'w',newline='',encoding='utf-8') as csvfile:
			writer=csv.DictWriter(csvfile,fieldnames=field_name)
			writer.writeheader() #for header in csv file
			for entry in data:
				writer.writerow({field:entry.get(field) for field in field_name})

		print(f"Data fetched successfully and written to '{csv_filename}'")

		'''# Upload the CSV file to GCS
		bucket_name = 'bkt-cricket-ranking'
		storage_client = storage.Client()
		bucket = storage_client.bucket(bucket_name)
		destination_blob_name = f'{csv_filename}'  # The path to store in GCS

		blob = bucket.blob(destination_blob_name)
		blob.upload_from_filename(csv_filename)'''
	else:
		print("No data available from API")
else:
	print("Failed to fetch data:",response.status_code)


#print(response.json())

import requests
import datetime

# set the base URL of the REST API
base_url = "https://example.com/api/data"

# set the end time as the current time
end_time = datetime.datetime.now()

# retrieve all data for the past 3 days, in batches of 100 rows or less
data = []
batch_size = 100
offset = 0
while True:
    # set the query parameters for this batch
    params = {
        "start_time": (end_time - datetime.timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_time": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "limit": batch_size,
        "offset": offset
    }

    # send the GET request to the API
    response = requests.get(base_url, params=params)

    # check if the request was successful
    if response.status_code == 200:
        # parse the response data as JSON and append to the data list
        batch_data = response.json()
        data.extend(batch_data)
        # if we retrieved less than the batch size, we have all the data
        if len(batch_data) < batch_size:
            break
        # update the offset for the next batch
        offset += batch_size
    else:
        # handle the error
        print(f"Error: {response.status_code} - {response.text}")
        break

# do something with the data
print(data)
- J adopts a customer-centric approach, placing the customer at the core of all his business activities. One invaluable lesson gleaned from working with him is his adeptness in establishing effective feedback loops, enabling a profound understanding of customer needs and challenges.

- J demonstrates exceptional global awareness in the Robotic Process Automation (RPA) market, possessing an insightful grasp of market trends and identifying promising opportunities.

- While J possesses a clear vision for the company's RPA initiatives, his drive occasionally outpaces the team's ability to keep up, highlighting the need for synchronization between his vision and the team's capabilities.

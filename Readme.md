### Instructions for running the code

pre reqs: python3, docker

`docker-compose up -d` will start the postgres container in the background

creating a virtual env is recommended

`python3 -m vene env`

`source env/bin/activate`

the following will install the required library
`pip install -r requirements.txt`

`python ./pipeline/main.py` will run the code and print the results as well as write them to a file named answer.txt in the reuslts directory

The approach to the problem I have taken:

At first, I analyzed the schema from the 3 sources to know what fields I am dealing with.

The questions were around zip code and providers. The web data and the csv file data contained deatiled information about the providers while the api
contained information about the contact person for the providers. Then I implemneted the code to insert the data based on the common schema that I could form.

The schema I defined for providers is 
` 
provider_name, type_of_care, address, city, state, zip , phone, email, owner_name` with the 
PRIMARY KEY  as (provider_name, zip)`

As per my analysis I believed provider name can be present in two addresses so I took the combination of these two to identify a unique row.

The api data was inserted into the db based on the match with email, phone and name and the owners name was updated.
The api data did not contain address information so as per my analysis I thought updating more information on rows where phone, email mathced was the best solution.

The python code has a main function which runs all code sequentially with web first, then csv and finally the api update.
If I had more time a airflow dag could have been defined where first step will be web data, second csv and the third step is api data.

The airflow module can be run as per cron schedule job.

At the end of the insertion I calculate the statistics required and store them in a text file. Those can be chained to the DAG as well and 
the result can be populated into a desired location or db table.

The first question asks about number of providers. So I grouped by name, type and zip.
If it had asked number of unique provider names then it would have been count of groups name and type.

For test I could not get time to write detailed unit test cases and functional validations. 
However approach will be to mock several services to test the respective methods. 
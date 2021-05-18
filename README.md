## Data Engineering Assignments

Question 1 solution steps:

1- After getting the container up and running, open http://localhost:8080/nifi/

2- From the Nifi home page, click on the uplaod template icon highlighted below ![Screen Shot 2021-05-18 at 22 07 06](https://user-images.githubusercontent.com/77297836/118709424-9d762b00-b825-11eb-9240-16e8e411295b.png) then select the "Assignment_Q1.xml" file

3- After the template is uplaoded, load the template from the template icon ![Screen Shot 2021-05-18 at 22 10 51](https://user-images.githubusercontent.com/77297836/118709674-f5ad2d00-b825-11eb-8563-c9ba21da8d8f.png)

4- Run the flow

5- Put the CSV file that you want to convert to Json in the /input directory

6- Open the /output directory to see the generated Json file ![Screen Shot 2021-05-18 at 22 12 58](https://user-images.githubusercontent.com/77297836/118709888-3b69f580-b826-11eb-8425-3f76f3b35c9c.png)


Question 2 Solution Steps:

1- After getting the container up and running, open http://localhost:8080/home/

2- On the Airflow home page, search for the dag that has been created in the /dags directory

3- Open the Dag, it should look like the following 

<img width="360" alt="Screen Shot 2021-05-18 at 21 25 35" src="https://user-images.githubusercontent.com/77297836/118711227-02cb1b80-b828-11eb-99c6-03d4fb23a18a.png">


<img width="1382" alt="Screen Shot 2021-05-18 at 21 14 48" src="https://user-images.githubusercontent.com/77297836/118711444-4756b700-b828-11eb-9229-f7a4dcd5c1e1.png">


4- Run the dag and check the results in the mongoDB using the link http://localhost:8081/

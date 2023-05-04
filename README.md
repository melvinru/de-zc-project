# de-zc-project
# This is the final project for Data Engineering ZoomCamp 2023

I have selected a dataset containing climate data because it is an essential and relevant topic in today's world. Climate change has a significant impact on our environment, economy, and society. By analyzing this data and presenting it on a dashboard, we can help raise awareness about climate change, its consequences, and the need for sustainable practices.

The climate data will provide valuable insights into historical and current temperature trends, precipitation patterns, and extreme weather events. On the dashboard, I will display some tiles of minimal, median and maximum temperature.

For the example, I took the coldest place on earth, called Ojmjakon it's in Siberia, Republic of Sakha, to make it more indicative.
![Ojmjakon it's in Siberia, Republic of Sakha](media/Ojmjakon%20details.png)

The difference of more than 5 degrees Celsius between 1943 and 2023 is enormous.
For minimal temperature I took 2 coldest moth in the year, january and february.
For average temperature I calculate rolling yearly average temperature.
A visual representation of temperature changes over time, highlighting any trends or anomalies. 

These visualizations will touch on problems related to global warming, its effects on ecosystems, and the increasing frequency of extreme weather events. Later I will also shed light on regional disparities in the impacts of climate change.

# Dashboard
You can see the dashboard [here](https://lookerstudio.google.com/reporting/5401162a-f822-497c-b1e0-1eea69d28f05)

On the first tile we see that left part is mostly higher the mediana.
On third screen I calculate the rolling yearly average temperature.
![minimal, mediana, rolling yearly avg temperature](media/Climate%20change%20as%20exemplified%20by%20the%20coldest%20place%20on%20earth,%20Ojmjakon%C2%A0%E2%80%BA%20Minimal%20and%20rolling%20average%20temperature%20-%20Vivaldi_230504125612.png)

This tile shows that recent years are generally warmer than previous years.
![This tile shows that recent years are generally warmer than previous years](media/Climate%20change%20as%20exemplified%20by%20the%20coldest%20place%20on%20earth,%20Ojmjakon%C2%A0%E2%80%BA%20Maximal%20temperature%20in%20coldest%20place%20-%20Vivaldi_230504125552.png)

Now, let's proceed with the project by creating the necessary pipelines and transforming the data:

1. Data Processing Pipeline: Using GCP and Prefect, we will create a pipeline to fetch the climate data, preprocess it (cleaning, filtering, and aggregating), and store it in a data lake (Google Cloud Storage).
2. Data movement pipeline: Next, we will create another pipeline using Terraform and Prefect to move the processed data from the data lake to a data warehouse (BigQuery). This pipeline ensures that the data is organized and readily available for further analysis.
3. Data Transformation: With the data in BigQuery, we will use dbt to perform a series of transformations to prepare it for the dashboard. This include aggregating the data by time periods, calculating averages and trends, and normalizing the data for better comparison.
4. Dashboard creation: Finally, we will use a data visualization tool (e.g., Google Data Studio) to create a visually appealing and informative dashboard with the two tiles mentioned earlier. The dashboard will be interactive, allowing users to explore the data and draw their own conclusions about climate change and its impacts.

## Step 1: Data Processing Pipeline
In this step, we will create a pipeline using GCP and Prefect to fetch the climate data, preprocess it, and store it in a data lake (Google Cloud Storage).

[Python pipeline web to gcs](flows/etl_web_to_gcs.py)

## Step 2: Data movement pipeline
Create a pipeline for moving the data from the lake to a data warehouse (BigQuery)

[Python pipeline gcs to bq](flows/etl_gcs_to_bq.py)

## Step 3: Data transformation
Transform the data in the data warehouse with dbt tool to prepare it for the dashboard.

[DBT Model](/de-zc-project/dbt)

## Step 4: Dashboard creation
Create a dashboard to visualize the transformed climate data.


## How to reproduce

### Settings up project
Create the new project in Google Cloud Console

### Setting up the environment on cloud VM
    Generating SSH keys
    Creating a virtual machine on GCP
    Connecting to the VM with SSH
    Installing Anaconda
    Installing Docker
    Creating SSH config file
    Accessing the remote machine with VS Code and SSH remote
    Installing docker-compose
    Installing pgcli
    Port-forwarding with VS code: connecting to pgAdmin and Jupyter from the local computer
    Installing Terraform
    Create Service Account, grant the following roles:
        Viewer
        Storage Admin
        Storage Object Admin
        BigQuery Admin
    Creating JSON key
    Using sftp for putting the credentials to the remote machine

[Requirements](requirements.txt)

[Terraform](/de-zc-project/terraform)

[Docker compose](/de-zc-project/docker)

[Jupyter notebook](flows/Untitled.ipynb)

### Creating a dashboard
I choosed Looker studio dashboard tool to visualize the transformed climate data stored in BigQuery.

Connecting dashboard tool to BigQuery
1. Log in to [Google Looker Studio](https://lookerstudio.google.com/navigation/reporting)
2. Create a new report by clicking on the `+` icon or the `Create` button.
3. In the "Add data to report" panel, search for "BigQuery" and click on the BigQuery connector.
4. Select your Google Cloud project, dataset, and the transformed climate data table (e.g., `de_zc_project_dev.climate`, `de_zc_project_dev.stg_climate` or `de_zc_project_dev.stg_cold_climate`).
5. Click the "Add" button to add the data source to your report.

Design dashboard
1. Using the available visualisation options in the dashboard tool, create the necessary charts, graphs and tables to display the transformed climate data.
2. Position the visualisations on the dashboard to create a clear and informative layout. 
3. Customise the appearance of the dashboard by changing colours, fonts and other design elements as required.

Share your dashboard
In Google Looker Studio, to share your dashboard, you need to press the "Share" button in the top right corner of the screen.
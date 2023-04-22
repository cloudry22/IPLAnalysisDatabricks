IPL Analysis using Azure Databricks
This project aims to analyze Indian Premier League (IPL) data using Azure Databricks. The project leverages the power of Apache Spark, a fast and general-purpose cluster computing system, to analyze large datasets.

Architecture Diagram
IPL Data Analysis using Databricks

The architecture diagram above depicts the flow of data in the project. The IPL data is sourced from https://cricsheet.org/downloads/. The data is then ingested into Azure Blob Storage, which serves as a data lake. Azure Databricks is used to read and process the data, and the results of the analysis are stored in Azure SQL Database.

Data Ingestion
The IPL data is downloaded from https://cricsheet.org/downloads/ in YAML format. The data is then converted to CSV format using Python scripts, and uploaded to Azure Blob Storage.

Data Processing
Azure Databricks is used to process the IPL data. The data is read from Azure Blob Storage, and processed using Spark DataFrames. Various analyses are performed on the data, including but not limited to:

Top batsmen and bowlers of the tournament
Team-wise and player-wise statistics
Analysis of player performance in various scenarios
Data Visualization
The results of the analysis are stored in Azure SQL Database, and are visualized using Power BI. Interactive dashboards are created to display the results of the analysis.

Conclusion
This project demonstrates the power of Azure Databricks in analyzing large datasets. The IPL data is used as an example, but the techniques used in this project can be applied to any dataset. The results of the analysis can be used to gain insights and make data-driven decisions.

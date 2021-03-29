# occupation-productivity
<h2> Introduction </h2>

GitHub repository for research on variations in the productivity of occupations. 
<h3> Project Goal </h3>

The goal of this project is to explore a syptom of Baumol's Cost Disease, which is how much wage changes in one occupation, caused by a productivity increase in that occupation (ie: the introduction of a new technology that makes workers more productive) affects the wage in another occupation. 
Specifically, we plan to group occupations together based off of shared skills that occupations are said to require, as indicated in O*NET Data. 

<h3> Overview </h3>

Here is an explanation of what is contained within this repository. There are two main folders - **Cleaning** and **Datasets**. **Cleaning** contains the R scripts that cleans and creates the datasets that will be used in the analysis. **Datasets** contains two subfolders, **Imported** and **Cleaned**. **Datasets/Imported** contains the raw data files processed by the scripts in **Cleaning**. **Datasets/Cleaned** contains the cleaned datasets created by the scrips in **Cleaning**.
<h2> Datasets </h2>

There are three datasets contained in **Datasets/Imported**.
<h3> ACS Data </h3>

The first dataset is the ACS data from IPUMS located in **Datasets/Imported/ACS**. The particular details of my ACS extract can be found in the file **Datasets/Imported/ACS/usa_00008.cbk.txt**. Due to size restrictions on uploading files to GitHub, I have uploaded my ACS .dat file to a cloud server. In addition, there are also two crosswalks contained in **Datasets/Imported/ACS**. The first,  **Datasets/Imported/ACS/indnaics_crosswalk_2000_onward_without_code_descriptions.csv** contains a crosswalk of IND1990 to various INDNAICS codes (that cover the span of time from 2000 to 2019, the latest year from which ACS data is available) that have been used over the years. However, this crosswalk does not map INDNAICS codes to NAICS codes. The second crosswalk, **Datasets/Imported/ACS/2003indnaics.csv** contains a mapping from INDNAICS to the 2002 and 2007 NAICS codes. 
<h3> BLS Industry Data </h3>

The second dataset is the BLS Labor Productivity dataset located in **Datasets/Imported/Industry Data** and can be downloaded from <a href = "https://download.bls.gov/pub/time.series/ip/">here</a>. This dataset contains statistics on the labor productivity of various industries over time on various levels of granularity. Further informationc about this dataset can be found in **Datasets/Imported/Industry Data/ip.txt** which is the BLS's own summary of the dataset. 

<h3> ONET Data </h3>

The third dataset is ONET located in **Datasets/Imported/ONET Data** and can be downloaded from <a href = "https://www.onetcenter.org/db_releases.html">here</a>. In particular, because only one instance per year of the ONET database was needed, the database that corresponded to the latest available ONET data from that year (typically October or November of that year) was used. For further information, see the **Read Me.txt** located in each instance of the ONET database. The data downloaded covers time from 2003 to 2020. 
<h2> Cleaning and Cleaned Datasets </h2>
<h3> Cleaning BLS Industry Data </h3> 

The BLS data is handled by the script **Cleaning/readingLP.R**. What this script accomplishes is that it takes in all the BLS Labor Productivity data, filters for just the metrics that inform us the annual industry-specific labor productivity metrics and combines the various explanatory data spread out among all the data files into just one table. This script outputs two files, **Datasets/Cleaned/lp_current.csv** and **Datasets/Cleaned/lp_all.csv**. These datasets are identical except for the fact that **Datasets/Cleaned/lp_all.csv** goes from 1987 to 2019, while **Datasets/Cleaned/lp_current.csv** goes from 2001 to 2019. This difference occurs because the BLS raw data contains two separate data files that also make this distinction (**Datasets/Imported/Industry Data/ip.data.0.Current.txt** and **Datasets/Imported/Industry Data/ip.data.1.AllData.txt**). 

<h3> Cleaning the ONET data </h3> 

The ONET data is handled by the script **Cleaning/readingONET.R**. On a high level, this script accomplishes is that it takes in the various ONET datat files located in each dataset and merges occupations with their corresponding work activities/skills/abilities required. This creates a dataset which allows us to create groups of occupations based off similarities in the work activities they engage in, or the skills/abilities required to complete a certain job. This script outputs two files **Datasets/Cleaned/matching_dataset_abilities.csv** and **matching_dataset_skills.csv**. Each file contains occupations, the work activities required by the occupation, the score of the ability (for the first file), or the skill (for the second file) that survey responders indicated the job needed and other assorted information. These files have not been uploaded to GitHub because of the file size restriction - instead, their zip file equivalents have been uploaded. 
<h3> Cleaning the ACS data </h3> 

The ACS data is handled by the script **Cleaning/readingACS.R**. This script produces three deliverables. The first is a file called **Datasets/Cleaned/lp_naics.csv** which lists the NAICS codes of all the industries for which we have BLS Data on productivity for. The second is a file called **Datasets/Cleaned/count_occsoc_indnaics.csv** which contains percentage information on the NAICS industries that people in certain occupations are part of, based off the results of the ACS data. The third file **Datasets/Cleaned/count_occsoc_indnaics_year.csv** is similar to the second, except with the added detail that the data is broken down into year as well. 
<h3> Creating the BLS-ACS crosswalk </h3> 

The creation of this crosswalk is handled by the script **Cleaning/crosswalk.R**. What this crosswalk does is match the INDNAICS codes from the ACS data to the NAICS code used by the BLS Labor Productivity data. In particular, this crosswalk only conatins the INDNAICS codes that can be mapped to a NAICS code to the BLS Labor Productivity data, and vice versa. Not all INDNAICS codes can be mapped to a NAICS code which the BLS contains industry productivity data on (notable examples includes industries that start with 1 or 9). This script produces the deliverable **Datasets/Cleaned/full_crosswalk.csv** that contains this crosswalk. 


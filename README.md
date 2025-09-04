# Social-graph-miner: multi-platform-data-analysis

In the repository there is a folder for each dataset on which the analyis has been conducted: 
- Blueksy
- Mastodon

Each social media folder contains: 
- dataset/
  - 100_posts/
    - 02_2024.json
    - 03_2024.json
    - ...
    - 07/2025.json
      
  - random/
    - 02_2024.json
    - 03_2024.json
    - ...
    - 07/2025.json
      
- code/
  - dataset/
    - 100_posts/
      - download_100.py
      - post_processing_100.ipynb
        
    - random/  
      - download_random.py
      - post_processing_random.ipynb

Code contains two folders: 
- dataset: contains the scripts to extract and post process the experimental dataset
- random : contains the scripts to extract and post process the control dataset 

# Steps: 
1. download requirements
2. create a profile on both bluesky and mastodon
3. retrieve tokens and copy them in env.
4. run download.py & download_random.py
5. run post_processing_100.ipynb & post_processing_random

   


  

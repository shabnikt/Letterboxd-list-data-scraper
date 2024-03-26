# Letterboxd-list-data-scraper

The script is an ETL tool that connects to your Letterboxd account and retrieves information about your lists and the movies in them.

The script writes the data to Postgres db and is needed to run Letterboxd film picker UI program.

Initially, the script exports the csv files of all your lists and starts working with them. You can specify the names of lists to ignore when filling the database in the ignore_list.txt file. In the environment variables file, if you specify True near the corresponding variable, the script will create this file with all list names and you can remove the ones you are interested in.

In create_database.sql you will find the necessary query to create the schema and tables. Then create letterboxd.env and fill it with the necessary values. Set the dependencies using requirements.txt and run main.py


As a result, the program will retrieve all your movie lists, download new ones or new movies in the existing ones in the database. The script also parses various data necessary for correct operation of Letterboxd movie picker UI, such as images or streaming links.


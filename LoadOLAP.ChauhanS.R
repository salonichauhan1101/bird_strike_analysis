library(DBI)
library(readr)
library(RSQLite)
library(sqldf)
library(dplyr)

#Path to SQLite database file
db_path <- "mydb.sqlite"

#Creating connection to the SQLite database
con <- dbConnect(RSQLite::SQLite(), dbname = db_path)

# Fetch and aggregate data for products
products <- dbGetQuery(con,"SELECT 
  p.name AS product_name,
  r.territory,
  strftime('%Y', s.sale_date) AS year,
  strftime('%m', s.sale_date) AS month,  
  SUM(s.sale_amount) AS total_sale_amount,
  SUM(s.quantity) AS total_quantity
FROM 
  sales s
JOIN 
  products p ON p.product_id = s.product_id
JOIN 
  reps r ON r.rID = s.rep_id
GROUP BY
  p.name,
  r.territory,
  year,
  month")  


# Fetch and aggregate data for reps
reps_data <- dbGetQuery(con, "
SELECT 
  r.rID, 
  r.name AS rep_name,
  strftime('%Y', s.sale_date) AS year,
  CAST(strftime('%m', s.sale_date) AS INTEGER) AS month,
  SUM(s.sale_amount) AS total_sales_amount,
  AVG(s.sale_amount) AS average_sales_amount,
  COUNT(*) AS number_of_sales
FROM 
  reps r
JOIN 
  sales s ON r.rID = s.rep_id
GROUP BY
  r.rID, year, month
")

###########################---MySQL Database---################################

library(RMySQL)

dbcon <- dbConnect(RMySQL::MySQL(), dbname = 'sql5687417', host = 'sql5.freemysqlhosting.net',
                 username = 'sql5687417', password = 'j7GbnXm3gS')

#Dropping tables if exists
dbSendQuery(dbcon, "DROP TABLE IF EXISTS rep_facts")
dbSendQuery(dbcon, "DROP TABLE IF EXISTS product_facts")
dbSendQuery(dbcon, "DROP TABLE IF EXISTS time_dimension")

#Creating the time_dimension table 
dbExecute(dbcon, "
CREATE TABLE time_dimension (
  time_id INT AUTO_INCREMENT PRIMARY KEY,
  month INT NOT NULL,
  year INT NOT NULL
)")

#Creating the product_facts table 
dbExecute(dbcon,"CREATE TABLE product_facts (
  fact_id INT AUTO_INCREMENT PRIMARY KEY,
  product_name VARCHAR(255),
  territory VARCHAR(255),
  time_id INT,
  total_sale_amount DOUBLE,
  total_quantity INT,
  FOREIGN KEY (time_id) REFERENCES time_dimension(time_id)
)")

# Creating the rep_facts table 
dbExecute(dbcon, "
CREATE TABLE IF NOT EXISTS rep_facts (
  rep_fact_id INT AUTO_INCREMENT PRIMARY KEY,
  rID INT,
  rep_name VARCHAR(255),
  time_id INT,
  total_sales_amount DOUBLE,
  average_sales_amount DOUBLE,
  number_of_sales INT,
  FOREIGN KEY (time_id) REFERENCES time_dimension(time_id)
)")

# Generating the date sequence
start_year <- 2000
end_year <- 2025
dates <- seq(as.Date(paste(start_year, "01", "01", sep="-")), as.Date(paste(end_year, "12", "31", sep="-")), by="month")

# Create data frame for insertion
time_data <- data.frame(
  month = as.integer(format(dates, "%m")),
  year = as.integer(format(dates, "%Y"))
)

# Remove duplicate entries if they exist
time_data <- unique(time_data)

# Inserting data into time_dimension table
dbWriteTable(dbcon, "time_dimension", time_data, append = TRUE, row.names = FALSE)

df_time <- dbGetQuery(dbcon, "SELECT * FROM time_dimension")

# Adjusting month formatting in the 'products' dataframe
products$month <- as.integer(as.character(products$month))

# Map time_id based on year and month in products
products$time_id <- with(products, {
  yearMonth <- paste(year, month, sep="-")
  time_ids <- setNames(df_time$time_id, paste(df_time$year, df_time$month, sep="-"))
  time_ids[yearMonth]
})


products <- subset(products, select = -c(year, month))

# Inserting data into product_facts table
dbWriteTable(dbcon, "product_facts", products, append = TRUE, row.names = FALSE)


##############################################################################

# Map time_id based on year and month in reps_data
reps_data$time_id <- with(reps_data, {
  yearMonth <- paste(year, month, sep="-")
  time_ids <- setNames(df_time$time_id, paste(df_time$year, df_time$month, sep="-"))
  time_ids[yearMonth]
})

# Select necessary columns to match the MySQL table structure
reps_data <- reps_data %>% 
  select(rID, rep_name, time_id, total_sales_amount, average_sales_amount,number_of_sales)


reps_data$rID <- as.integer(reps_data$rID)
reps_data$time_id <- as.integer(reps_data$time_id)
reps_data$total_sales_amount <- as.double(reps_data$total_sales_amount)
reps_data$average_sales_amount <- as.double(reps_data$average_sales_amount)
reps_data$number_of_sales <- as.integer(reps_data$number_of_sales)

# Inserting data into rep_facts table
dbWriteTable(dbcon, "rep_facts", reps_data, append = TRUE, row.names = FALSE)

dbDisconnect(dbcon)

library(RSQLite)
library(XML)
library(DBI)

#Path to SQLite database file
db_path <- "mydb.sqlite"

# Creating a connection to the SQLite database
con <- dbConnect(RSQLite::SQLite(), dbname = db_path)

# Dropping the existing tables
tables <- c("products", "reps", "customers", "sales")
for (table in tables) {
  dbSendQuery(con, sprintf("DROP TABLE IF EXISTS %s", table))
}

# Create new tables products,reps,customers,sales

dbExecute(con, "
CREATE TABLE products (
  product_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  description TEXT
)")
dbExecute(con, "
CREATE TABLE reps (
  rID INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  territory TEXT,
  commission_rate REAL
)")
dbExecute(con, "
CREATE TABLE customers (
  customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  country TEXT NOT NULL
)")
dbExecute(con, "
CREATE TABLE sales (
  sales_id INTEGER PRIMARY KEY AUTOINCREMENT,
  txn_id INTEGER,
  product_id INTEGER,
  rep_id INTEGER,
  customer_id INTEGER,
  quantity INTEGER,
  sale_amount REAL,
  sale_date DATE,
  FOREIGN KEY(product_id) REFERENCES products(product_id),
  FOREIGN KEY(rep_id) REFERENCES reps(rID),
  FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
)")

# Initialize empty data frames
df_products <- data.frame(name=character(), description=character(), stringsAsFactors=FALSE)
df_reps <- data.frame(rID=character(), name=character(), territory=character(), commission_rate=numeric(), stringsAsFactors=FALSE)
df_customers <- data.frame(name=character(), country=character(), stringsAsFactors=FALSE)
df_sales <- data.frame()

#Parsing the XML files for reps
reps_xml <- xmlParse("txn-xml/pharmaReps-F23.xml")

#Populating df_reps from XML
reps_nodes <- getNodeSet(reps_xml, "//rep")
for (node in reps_nodes) {
  rID <- gsub("r", "", xmlGetAttr(node, "rID"))
  name <- paste(xpathSApply(node, "./name/first/text()", xmlValue), xpathSApply(node, "./name/sur/text()", xmlValue))
  territory <- xpathSApply(node, "./territory/text()", xmlValue)
  commission_rate <- as.numeric(xpathSApply(node, "./commission/text()", xmlValue))
  df_reps <- rbind(df_reps, data.frame(rID=rID, name=name, territory=territory, commission_rate=commission_rate, stringsAsFactors=FALSE))
}

#Removing duplicate records in df_reps
df_reps <- unique(df_reps)

#Parsing the XML files for transactions
txn_files <- c("txn-xml/pharmaSalesTxn-10-F23.xml", "txn-xml/pharmaSalesTxn-20-F23.xml", "txn-xml/pharmaSalesTxn-3000-F23.xml", 
               "txn-xml/pharmaSalesTxn-5000-F23.xml","txn-xml/pharmaSalesTxn-8000-F23.xml")

#Processing each XML file for transactions
for (file in txn_files) {
  sales_xml <- xmlParse(file)
  txn_nodes <- getNodeSet(sales_xml, "//txn")
  for (node in txn_nodes) {
    product_name <- xpathSApply(node, "./sale/product/text()", xmlValue)
    customer_name <- xpathSApply(node, "./customer/text()", xmlValue)
    country <- xpathSApply(node, "./country/text()", xmlValue)
    df_customers <- rbind(df_customers, data.frame(name=customer_name, country=country, stringsAsFactors=FALSE))
    df_products <- rbind(df_products, data.frame(name=product_name, description=NA, stringsAsFactors=FALSE))
    
    txn_id <- as.integer(xmlGetAttr(node, "txnID"))
    rep_id <- xmlGetAttr(node, "repID")
    quantity <- as.integer(xpathSApply(node, "./sale/qty/text()", xmlValue))
    sale_amount <- as.numeric(xpathSApply(node, "./sale/total/text()", xmlValue))
    sale_date <- xpathSApply(node, "./sale/date/text()", xmlValue)
    
    
    # Appending new rows to df_sales with additional product and customer name for matching later
    df_sales <- rbind(df_sales, data.frame(txn_id=txn_id, rep_id=rep_id, product_name=product_name,
                                           customer_name=customer_name, quantity=quantity,
                                           sale_amount=sale_amount, sale_date=sale_date, stringsAsFactors=FALSE))  }
}

# Deduplicate df_customers and df_products based on names
df_customers <- unique(df_customers)
df_products <- unique(df_products)

# Assigning IDs to customers,products and sales
df_customers$customer_id <- seq.int(nrow(df_customers))
df_products$product_id <- seq.int(nrow(df_products))
df_sales$sales_id <- seq.int(nrow(df_sales))

# Updating df_sales with correct foreign keys
df_sales$product_id <- df_products$product_id[match(df_sales$product_name, df_products$name)]
df_sales$customer_id <- df_customers$customer_id[match(df_sales$customer_name, df_customers$name)]

# Formatting date correctly before insertion
df_sales$sale_date <- format(as.Date(df_sales$sale_date, format="%m/%d/%Y"), "%Y-%m-%d")

df_sales <- subset(df_sales, select = -c(product_name, customer_name))

# Inserting data into the database
dbWriteTable(con, "reps", df_reps, append=TRUE, row.names=FALSE)
dbWriteTable(con, "products", df_products, append=TRUE, row.names=FALSE)
dbWriteTable(con, "customers", df_customers, append=TRUE, row.names=FALSE)
dbWriteTable(con, "sales", df_sales, append=TRUE, row.names=FALSE)

# Disconnect from the database
dbDisconnect(con)




# DXP Project: DataOps-Bot Checking Products After ETL

## Decscription 

The project's aim is to validate all product categories using UPC and Store codes, ensuring the complete accuracy of data displayed on the Frontend, aligning with the information stored in the database.

With a large quantity of products, approximately over a million rows processed daily through ETL, it is necessary to employ a technique to run the verification program comprehensively and complete it within a reasonable timeframe. This ensures that product information is accurate, allowing online shoppers to make purchases without any issues.

Based on that, it is necessary to write an endpoint to run the code daily after the ETL process is complete for verification. The API is written using the Python framework FastAPI.

data = {
    # ... (product data)
}
Required fields to verify
UPC
STORE_CODE
PRICE
NAME
CATEGORY
DESCRIPTION
IMAGE
From the UPC and Store codes, we proceed to retrieve information for the mentioned fields by calling an API to return the data.

API to get data
https://../scrsecom/v2.0/api/Product/search


Workflow Details
Source file: price_sale_of_product.csv and price_regular_of_product.csv
Destination file: price_sale_of_product_check.csv and price_regular_of_product_check.csv
Both the input and output files are located within the same container.
Implementation Workload
The bot is run on a server that has been configured and deployed with code on Kubernetes (K8s), along with Jenkins and Docker. Continuous Integration/Continuous Deployment (CI/CD) processes are set up to trigger when code is committed to Git.

Notification
Check notifications in the webhook after the execution is complete.

Conclusion
To manually execute this bot, we could use the following endpoints:

https://../product_sale_check/
https://../product_regular_check/


## Contact

For questions and feedback, please contact:

- [Tam Ho]
- Email: [hongoctam0812@gmail.com]
- GitHub: [Your GitHub Profile](https://github.com/hntam812)

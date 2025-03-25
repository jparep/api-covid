# API Connection to COVID Data in Snowflake

This project establishes a connection to retrieve COVID-19 data from an API and load it into a Snowflake database for further analysis.

## Features

- Fetch COVID-19 data from a public API.
- Transform and prepare the data for Snowflake ingestion.
- Load the data into Snowflake tables for querying and analysis.

## Prerequisites

- Python 3.8 or higher
- Snowflake account and credentials
- Required Python libraries (see `requirements.txt`)

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/your-repo/api-covid.git
    cd api-covid
    ```

2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Configure Snowflake credentials in a `.env` file:
    ```
    SNOWFLAKE_USER=<your_username>
    SNOWFLAKE_PASSWORD=<your_password>
    SNOWFLAKE_ACCOUNT=<your_account>
    SNOWFLAKE_DATABASE=<your_database>
    SNOWFLAKE_SCHEMA=<your_schema>
    ```

## Usage

1. Run the script to fetch and load COVID-19 data:
    ```bash
    python load_covid_data.py
    ```

2. Verify the data in your Snowflake database:
    ```sql
    SELECT * FROM <your_table>;
    ```

## Project Structure

- `load_covid_data.py`: Main script to fetch and load data.
- `requirements.txt`: List of dependencies.
- `.env`: Configuration file for Snowflake credentials.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Acknowledgments

- COVID-19 data provided by [API Source Name].
- Snowflake for data warehousing solutions.

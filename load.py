from dagster import op, Out, In, get_dagster_logger
from sqlalchemy import create_engine, exc, text
from sqlalchemy.pool import NullPool
import pandas as pd
import psycopg2
from urllib.parse import quote_plus
from sqlalchemy import create_engine

logger = get_dagster_logger()


def create_database_and_tables():
    connection_string = "postgresql://postgres:dap@localhost:5432/postgres"
    try:
        default_engine = create_engine(connection_string)
        with default_engine.connect() as default_connection:
            default_connection.execution_options(isolation_level="AUTOCOMMIT").execute(
                text("CREATE DATABASE Alternative_Fuel_Stations;")
            )
        logger.info("Database created successfully.")
        conn = psycopg2.connect(database="alternative_fuel_stations", user='postgres', password='dap', host='localhost',
                                port='5432')
        conn.autocommit = True
        cursor = conn.cursor()
        light_duty_sql = """DROP TABLE IF EXISTS public.light_duty_vehicle;
                                CREATE TABLE public.light_duty_vehicle
                                (
                                    _id text NOT NULL,
                                    id text NOT NULL,
                                    fuel_id text NOT NULL,
                                    light_duty_manufacturer_id text NOT NULL,
                                    light_duty_category_id text NOT NULL,
                                    model text NOT NULL,
                                    model_year text NOT NULL,
                                    transmission_type text,
                                    manufacturer_name text NOT NULL,
                                    fuel_code text NOT NULL,
                                    fuel_name text NOT NULL,
                                    light_duty_fuel_configuration_name text,
                                    category_name text NOT NULL,
                                    fuel_economy_estimated_by_manufacturer text NOT NULL
                                );"""
        cursor.execute(light_duty_sql)
        logger.info("light_duty_vehicle table created successfully.")
        conn.commit()

        incentive_laws_sql = """DROP TABLE IF EXISTS public.incentive_laws;
                                CREATE TABLE public.incentive_laws
                                (
                                    _id text NOT NULL,
                                    id text NOT NULL,
                                    state text NOT NULL,
                                    title text NOT NULL,
                                    is_recent text,
                                    type text NOT NULL,
                                    recent_update_or_new text NOT NULL,
                                    county_ids text NOT NULL,
                                    technologies text,
                                    technology_titles text NOT NULL,
                                    categories text NOT NULL,
                                    types text NOT NULL
                                );"""
        cursor.execute(incentive_laws_sql)
        logger.info("incentive_laws table created successfully.")
        conn.commit()
        conn.close()
        return True
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False


db_cred = {
    'host': 'localhost',
    'database': 'alternative_fuel_stations',
    'user': 'postgres',
    'password': 'dap',
    'port': '5432'
}
csv_file_path = 'csv_file/alt_fuel_stations.csv'
table_name = 'public.alt_fuel_stations'


def save_csv_to_postgresql(csv_file_path, table_name, db_cred):
    try:
        df = pd.read_csv(csv_file_path, low_memory=False)
        columns_to_retain = ['Fuel Type Code', 'Station Name', 'Street Address', 'City', 'State', 'ZIP', 'Status Code',
                             'Geocode Status', 'Latitude', 'Longitude', 'ID', 'Updated At', 'Open Date', 'Country',
                             'Access Code']
        columns_with_null = df.columns[df.isnull().any()]
        columns_to_drop = [col for col in columns_with_null if col not in columns_to_retain]
        Fuel_df = df.drop(columns=columns_to_drop)
        # Find out most frequent value of street address
        most_frequent_value = Fuel_df['Street Address'].mode()[0]
        Fuel_df['Street Address'].fillna(most_frequent_value, inplace=True)

        # Find out most frequent value of Station Name
        most_frequent_value = Fuel_df['Station Name'].mode()[0]
        Fuel_df['Station Name'].fillna(most_frequent_value, inplace=True)

        # Find out most frequent Value of City
        most_frequent_value = Fuel_df['City'].mode()[0]
        Fuel_df['City'].fillna(most_frequent_value, inplace=True)

        # Find out most frequent Value of State
        most_frequent_value = Fuel_df['State'].mode()[0]
        Fuel_df['State'].fillna(most_frequent_value, inplace=True)

        # Find Out most frequent value of Geocode Status
        most_frequent_value = Fuel_df['Geocode Status'].mode()[0]
        Fuel_df['Geocode Status'].fillna(most_frequent_value, inplace=True)

        # Find Out most frequent value of Latitude
        most_frequent_value = Fuel_df['Latitude'].mode()[0]
        Fuel_df['Latitude'].fillna(most_frequent_value, inplace=True)

        # Find Out most frequent value of Longitude
        most_frequent_value = Fuel_df['Longitude'].mode()[0]
        Fuel_df['Longitude'].fillna(most_frequent_value, inplace=True)

        # Find Out most frequent value of ID
        most_frequent_value = Fuel_df['ID'].mode()[0]
        Fuel_df['ID'].fillna(most_frequent_value, inplace=True)

        # Find Out most frequent value of Updated At
        most_frequent_value = Fuel_df['Updated At'].mode()[0]
        Fuel_df['Updated At'].fillna(most_frequent_value, inplace=True)

        # Find Out most frequent value of Open Date
        most_frequent_value = Fuel_df['Open Date'].mode()[0]
        Fuel_df['Open Date'].fillna(most_frequent_value, inplace=True)

        # Find Out most frequent value of Country
        most_frequent_value = Fuel_df['Country'].mode()[0]
        Fuel_df['Country'].fillna(most_frequent_value, inplace=True)

        # Find Out most frequent value of Access Code
        most_frequent_value = Fuel_df['Access Code'].mode()[0]
        Fuel_df['Access Code'].fillna(most_frequent_value, inplace=True)

        columns_to_drop = ['Groups With Access Code (French)', 'Longitude', 'Latitude']
        cleaned_df = Fuel_df.drop(columns=columns_to_drop)

        encoded_password = quote_plus(db_cred["password"])
        engine = create_engine(
            f'postgresql+psycopg2://{db_cred["user"]}:{encoded_password}@{db_cred["host"]}:{db_cred["port"]}/{db_cred["database"]}'
        )
        cleaned_df.to_sql(table_name, engine, if_exists='replace', index=False)
        logger.info(f"CSV file '{csv_file_path}' successfully saved to PostgreSQL table '{table_name}'.")
    except Exception as e:
        logger.error(f"Error: Unable to save CSV to PostgreSQL.\n{e}")


postgres_connection_string = "postgresql://postgres:dap@localhost:5432/alternative_fuel_stations"


@op(ins={'start': In(None)}, out=Out(bool))
def load_light_duty_vehicles_dimension(start):
    create_database_and_tables()
    save_csv_to_postgresql(csv_file_path, table_name, db_cred)
    load_incentive_laws_dimension(start)
    light_duty_vehicles = pd.read_csv("staging/transformed_light_duty_vehicles.csv", sep="\t")

    try:
        engine = create_engine(postgres_connection_string, poolclass=NullPool)
        with engine.connect() as connection:
            connection.execute(text("TRUNCATE public.light_duty_vehicle"))
            rowcount = light_duty_vehicles.to_sql(
                name="public.light_duty_vehicle",
                schema="public",
                con=engine,
                index=False,
                if_exists="append",
                method="multi",
                chunksize=3000
            )
        logger.info("%i light duty vehicle records loaded" % rowcount)
        engine.dispose(close=True)
        return rowcount > 0
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False


@op(ins={'start': In(None)}, out=Out(bool))
def load_incentive_laws_dimension(start):
    incentive_laws = pd.read_csv("staging/transformed_incentive_laws.csv", sep="\t")

    try:
        engine = create_engine(postgres_connection_string, poolclass=NullPool)
        with engine.connect() as connection:
            connection.execute(text("TRUNCATE public.incentive_laws"))
            rowcount = incentive_laws.to_sql(
                name="public.incentive_laws",
                schema="public",
                con=engine,
                index=False,
                if_exists="append",
                method="multi",
                chunksize=1000
            )
        logger.info("%i incentive laws records loaded" % rowcount)
        engine.dispose(close=True)
        return rowcount > 0
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False

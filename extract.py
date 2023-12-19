from pymongo import MongoClient
from dagster import op, Out, In, get_dagster_logger
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
import pandas as pd
import pymongo
import requests

client = pymongo.MongoClient("mongodb://dap:dap@localhost:27017/?authMechanism=DEFAULT")
database_name = "Alternative_Fuel_Stations"
light_duty_collection_name = "light_duty_vehicle_collection"
incentive_laws_collection_name = "incentive_laws_collection"
database = client[database_name]
light_duty_collection = database[light_duty_collection_name]
incentive_laws_collection = database[incentive_laws_collection_name]

mongo_connection_string = "mongodb://dap:dap@127.0.0.1"
light_duty_api_url = "https://developer.nrel.gov/api/vehicles/v1/light_duty_automobiles.json?api_key=3oMuzeeRhaYCTCgOyMaWMUtRmkTGPgLqa24CjIs3&download=true"
light_duty_response = requests.get(light_duty_api_url)
incentive_laws_api_url = "https://developer.nrel.gov/api/transportation-incentives-laws/v1.json?api_key=3oMuzeeRhaYCTCgOyMaWMUtRmkTGPgLqa24CjIs3&expired=false&incentive_type=GNT%2CTAX%2CLOANS%2CRBATE%2CEXEM%2CTOU%2COTHER&law_type=INC%2CPROG%2CLAWREG%2CSTATEINC%2CUPINC&regulation_type=REQ%2CDREST%2CREGIS%2CEVFEE%2CFUEL%2CSTD%2CRFS%2CAIRQEMISSIONS%2CCCEINIT%2CUTILITY%2CBUILD%2CRTC%2COTHER&technology=BIOD%2CETH%2CNG%2CLPG%2CHY%2CELEC%2CPHEV%2CHEV%2CNEVS%2CRD%2CAFTMKTCONV%2CEFFEC%2CIR%2CAUTONOMOUS%2COTHER&user_type=FLEET%2CGOV%2CTRIBAL%2CIND%2CSTATION%2CAFP%2CPURCH%2CMAN%2CMUD%2CTRANS%2COTHER"
incentive_laws_response = requests.get(incentive_laws_api_url)
logger = get_dagster_logger()

LightDutyDataFrame = create_dagster_pandas_dataframe_type(
    name="LightDutyDataFrame",
    columns=[
        PandasColumn.string_column("_id", non_nullable=True, unique=True),
        PandasColumn.string_column("id", non_nullable=True),
        PandasColumn.string_column("fuel_id", non_nullable=True),
        PandasColumn.string_column("phev_type"),
        PandasColumn.string_column("light_duty_fuel_configuration_id"),
        PandasColumn.string_column("light_duty_manufacturer_id", non_nullable=True),
        PandasColumn.string_column("light_duty_category_id", non_nullable=True),
        PandasColumn.string_column("model", non_nullable=True),
        PandasColumn.string_column("model_year", non_nullable=True),
        PandasColumn.string_column("photo_url"),
        PandasColumn.string_column("electric_range"),
        PandasColumn.string_column("total_range"),
        PandasColumn.string_column("transmission_type"),
        PandasColumn.string_column("engine_type"),
        PandasColumn.string_column("engine_size"),
        PandasColumn.string_column("engine_cylinder_count"),
        PandasColumn.string_column("engine_description"),
        PandasColumn.string_column("notes"),
        PandasColumn.string_column("manufacturer_name", non_nullable=True),
        PandasColumn.string_column("manufacturer_url", non_nullable=True),
        PandasColumn.string_column("fuel_code", non_nullable=True),
        PandasColumn.string_column("fuel_name", non_nullable=True),
        PandasColumn.string_column("light_duty_fuel_configuration_name"),
        PandasColumn.string_column("category_name", non_nullable=True),
        PandasColumn.string_column("alternative_fuel_economy_combined"),
        PandasColumn.string_column("conventional_fuel_economy_combined"),
        PandasColumn.string_column("fuel_economy_estimated_by_manufacturer", non_nullable=True),
        PandasColumn.string_column("drivetrain"),
        PandasColumn.string_column("charging_rate_level_2"),
        PandasColumn.string_column("charging_rate_dc_fast"),
        PandasColumn.string_column("charging_speed_level_1"),
        PandasColumn.string_column("charging_speed_level_2"),
        PandasColumn.string_column("charging_speed_dc_fast"),
        PandasColumn.string_column("battery_voltage"),
        PandasColumn.string_column("battery_capacity_amp_hours"),
        PandasColumn.string_column("battery_capacity_kwh"),
        PandasColumn.string_column("seating_capacity"),
        PandasColumn.string_column("alternative_fuel_economy_city"),
        PandasColumn.string_column("alternative_fuel_economy_highway"),
        PandasColumn.string_column("conventional_fuel_economy_city"),
        PandasColumn.string_column("conventional_fuel_economy_highway"),
        PandasColumn.string_column("light_duty_emission_certifications")
    ],
)

IncentiveLawsDataFrame = create_dagster_pandas_dataframe_type(
    name="IncentiveLawsDataFrame",
    columns=[
        PandasColumn.string_column("_id", non_nullable=True, unique=True),
        PandasColumn.string_column("id", non_nullable=True),
        PandasColumn.string_column("state", non_nullable=True),
        PandasColumn.string_column("title", non_nullable=True),
        PandasColumn.string_column("text"),
        PandasColumn.string_column("enacted_date"),
        PandasColumn.string_column("amended_date"),
        PandasColumn.string_column("plaintext"),
        PandasColumn.string_column("is_recent", non_nullable=True),
        PandasColumn.string_column("seq_num"),
        PandasColumn.string_column("type", non_nullable=True),
        PandasColumn.string_column("agency", non_nullable=True),
        PandasColumn.string_column("significant_update_date"),
        PandasColumn.string_column("recent_update_or_new", non_nullable=True),
        PandasColumn.string_column("utility_id"),
        PandasColumn.string_column("county_ids", non_nullable=True),
        PandasColumn.string_column("technologies", non_nullable=True),
        PandasColumn.string_column("technology_titles", non_nullable=True),
        PandasColumn.string_column("categories", non_nullable=True),
        PandasColumn.string_column("types", non_nullable=True),
        PandasColumn.string_column("references"),
        PandasColumn.string_column("topics"),
        PandasColumn.string_column("status"),
        PandasColumn.string_column("status_date")
    ],
)

@op(ins={'start': In(bool)}, out=Out(LightDutyDataFrame))
def extract_light_duty_vehicles(start) -> LightDutyDataFrame:
    storeLightVehicle()
    storeIncentiveLaws()
    conn = MongoClient(mongo_connection_string)
    db = conn["Dap_project_db"]
    light_duty_vehicles = pd.DataFrame(db.light_duty_collection.find({}))
    light_duty_vehicles['_id'] = light_duty_vehicles['_id'].astype(str)
    light_duty_vehicles['id'] = light_duty_vehicles['id'].astype(str)
    light_duty_vehicles['fuel_id'] = light_duty_vehicles['fuel_id'].astype(str)
    light_duty_vehicles['light_duty_fuel_configuration_id'] = light_duty_vehicles['light_duty_fuel_configuration_id'].astype(str)
    light_duty_vehicles['light_duty_manufacturer_id'] = light_duty_vehicles['light_duty_manufacturer_id'].astype(str)
    light_duty_vehicles['light_duty_category_id'] = light_duty_vehicles['light_duty_category_id'].astype(str)
    light_duty_vehicles['engine_cylinder_count'] = light_duty_vehicles['engine_cylinder_count'].astype(str)
    light_duty_vehicles['fuel_economy_estimated_by_manufacturer'] = light_duty_vehicles['fuel_economy_estimated_by_manufacturer'].astype(str)
    light_duty_vehicles['battery_voltage'] = light_duty_vehicles['battery_voltage'].astype(str)
    light_duty_vehicles['battery_capacity_amp_hours'] = light_duty_vehicles['battery_capacity_amp_hours'].astype(str)
    light_duty_vehicles['battery_capacity_kwh'] = light_duty_vehicles['battery_capacity_kwh'].astype(str)
    light_duty_vehicles['seating_capacity'] = light_duty_vehicles['seating_capacity'].astype(str)
    conn.close()
    return light_duty_vehicles

@op(ins={'light_duty_vehicles': In(LightDutyDataFrame)}, out=Out(None))
def stage_extracted_light_duty_vehicles(light_duty_vehicles):
    light_duty_vehicles.to_csv("staging/light_duty_vehicles.csv", index=False, sep="\t")

@op(out=Out(IncentiveLawsDataFrame))
def extract_incentive_laws() -> IncentiveLawsDataFrame:
    pipeline = [
        {'$match': {}},
        {'$project': {
            '_id': '$_id',
            'id': '$id',
            'state': '$state',
            'title': '$title',
            'text': '$text',
            'enacted_date': '$enacted_date',
            'amended_date': '$amended_date',
            'plaintext': '$plaintext',
            'is_recent': '$is_recent',
            'seq_num': '$seq_num',
            'type': '$type',
            'agency': '$agency',
            'significant_update_date': '$significant_update_date',
            'recent_update_or_new': '$recent_update_or_new',
            'utility_id': '$utility_id',
            'county_ids': '$county_ids',
            'technologies': '$technologies',
            'technology_titles': '$technology_titles',
            'categories': '$categories',
            'types': '$types',
            'references': '$references',
            'topics': '$topics',
            'status': '$status',
            'status_date': '$status_date'
        }}
    ]
    conn = MongoClient(mongo_connection_string)
    db = conn["Dap_project_db"]
    incentive_laws = pd.DataFrame(db.incentive_laws_collection.aggregate(pipeline))
    incentive_laws['_id'] = incentive_laws['_id'].astype(str)
    incentive_laws['id'] = incentive_laws['id'].astype(str)
    incentive_laws['technologies'] = incentive_laws['technologies'].astype(str)
    incentive_laws['is_recent'] = incentive_laws['is_recent'].astype(str)
    incentive_laws['seq_num'] = incentive_laws['seq_num'].astype(str)
    incentive_laws['utility_id'] = incentive_laws['utility_id'].astype(str)
    conn.close()
    return incentive_laws

@op(ins={'incentive_laws': In(IncentiveLawsDataFrame)}, out=Out(None))
def stage_extracted_incentive_laws(incentive_laws):
        incentive_laws.to_csv("staging/incentive_laws.csv", index=False, sep="\t")

def storeLightVehicle():
    api_data = None
    if light_duty_response.status_code == 200:
        api_data = light_duty_response.json()
    else:
        logger.error(f"Error: {light_duty_response.status_code}")

    if api_data and "result" in api_data:
        for item in api_data["result"]:
            if isinstance(item, dict):
                light_duty_collection.insert_one(item)
            else:
                logger.error(f"Skipping item: {item}, not a valid dictionary.")

def storeIncentiveLaws():
    api_data = None
    if incentive_laws_response.status_code == 200:
        api_data = incentive_laws_response.json()
    else:
        logger.error(f"Error: {incentive_laws_response.status_code}")

    if api_data and "result" in api_data:
        for item in api_data["result"]:
            if isinstance(item, dict):
                incentive_laws_collection.insert_one(item)
            else:
                logger.error(f"Skipping item: {item}, not a valid dictionary.")


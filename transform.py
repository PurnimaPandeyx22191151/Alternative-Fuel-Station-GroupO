from dagster import op, Out, In
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
import pandas as pd

TransformedLightDutyDataFrame = create_dagster_pandas_dataframe_type(
    name="TransformedLightDutyDataFrame",
    columns=[
        PandasColumn.string_column("_id", non_nullable=True, unique=True),
        PandasColumn.string_column("id", non_nullable=True),
        PandasColumn.string_column("fuel_id", non_nullable=True),
        PandasColumn.string_column("light_duty_manufacturer_id", non_nullable=True),
        PandasColumn.string_column("light_duty_category_id", non_nullable=True),
        PandasColumn.string_column("model", non_nullable=True),
        PandasColumn.string_column("model_year", non_nullable=True),
        PandasColumn.string_column("transmission_type", non_nullable=True),
        PandasColumn.string_column("manufacturer_name", non_nullable=True),
        PandasColumn.string_column("fuel_code", non_nullable=True),
        PandasColumn.string_column("fuel_name", non_nullable=True),
        PandasColumn.string_column("light_duty_fuel_configuration_name", non_nullable=True),
        PandasColumn.string_column("category_name", non_nullable=True),
        PandasColumn.string_column("fuel_economy_estimated_by_manufacturer", non_nullable=True),
    ],
)

@op(ins={'start':In(None)},out=Out(TransformedLightDutyDataFrame))
def transform_extracted_light_duty_vehicles(start) -> TransformedLightDutyDataFrame:
    light_duty_vehicles = pd.read_csv("staging/light_duty_vehicles.csv", sep="\t")
    light_duty_vehicles['_id'] = light_duty_vehicles['_id'].astype(str)
    light_duty_vehicles['id'] = light_duty_vehicles['id'].astype(str)
    light_duty_vehicles['fuel_id'] = light_duty_vehicles['fuel_id'].astype(str)
    light_duty_vehicles['light_duty_manufacturer_id'] = light_duty_vehicles['light_duty_manufacturer_id'].astype(str)
    light_duty_vehicles['light_duty_category_id'] = light_duty_vehicles['light_duty_category_id'].astype(str)
    light_duty_vehicles['model_year'] = light_duty_vehicles['model_year'].astype(str)
    light_duty_vehicles['fuel_economy_estimated_by_manufacturer'] = light_duty_vehicles['fuel_economy_estimated_by_manufacturer'].astype(str)
    most_frequent_value = light_duty_vehicles['transmission_type'].mode()[0]
    light_duty_vehicles['transmission_type'].fillna(most_frequent_value, inplace=True)
    most_frequent_value = light_duty_vehicles['light_duty_fuel_configuration_name'].mode()[0]
    light_duty_vehicles['light_duty_fuel_configuration_name'].fillna(most_frequent_value, inplace=True)
    columns_to_retain = ['transmission_type', 'light_duty_fuel_configuration_name']
    columns_with_null = light_duty_vehicles.columns[light_duty_vehicles.isnull().any()]
    columns_to_drop = [col for col in columns_with_null if col not in columns_to_retain]
    eda_df = light_duty_vehicles.drop(columns=columns_to_drop)
    columns_to_drop1 = ['light_duty_emission_certifications']  # Replace with your actual column names
    light_duty_vehicles = eda_df.drop(columns=columns_to_drop1)
    return light_duty_vehicles

@op(ins={'light_duty_vehicles': In(TransformedLightDutyDataFrame)}, out=Out(None))
def stage_transformed_light_duty_vehicles(light_duty_vehicles):
    light_duty_vehicles.to_csv(
        "staging/transformed_light_duty_vehicles.csv",
        sep="\t",
        index=False
    )

TransformedIncentiveLawsDataFrame = create_dagster_pandas_dataframe_type(
    name="TransformedIncentiveLawsDataFrame",
    columns=[
        PandasColumn.string_column("_id", non_nullable=True, unique=True),
        PandasColumn.string_column("id", non_nullable=True),
        PandasColumn.string_column("state", non_nullable=True),
        PandasColumn.string_column("title", non_nullable=True),
        PandasColumn.string_column("is_recent", non_nullable=True),
        PandasColumn.string_column("type", non_nullable=True),
        PandasColumn.string_column("recent_update_or_new", non_nullable=True),
        PandasColumn.string_column("county_ids", non_nullable=True),
        PandasColumn.string_column("technologies", non_nullable=True),
        PandasColumn.string_column("technology_titles", non_nullable=True),
        PandasColumn.string_column("categories", non_nullable=True),
        PandasColumn.string_column("types", non_nullable=True),
    ],
)

@op(ins={'start':In(None)},out=Out(TransformedIncentiveLawsDataFrame))
def transform_extracted_incentive_laws(start) -> TransformedIncentiveLawsDataFrame:
    incentive_laws = pd.read_csv("staging/incentive_laws.csv", sep="\t")
    incentive_laws['_id'] = incentive_laws['_id'].astype(str)
    incentive_laws['id'] = incentive_laws['id'].astype(str)
    incentive_laws['technologies'] = incentive_laws['technologies'].astype(str)
    incentive_laws['is_recent'] = incentive_laws['is_recent'].astype(str)
    incentive_laws['utility_id'] = incentive_laws['utility_id'].astype(str)
    eda_df = incentive_laws.dropna(axis=1)
    columns_to_drop = ['utility_id', 'text', 'plaintext', 'references', 'topics']
    incentive_laws = eda_df.drop(columns=columns_to_drop)
    return incentive_laws

@op(ins={'incentive_laws': In(TransformedIncentiveLawsDataFrame)}, out=Out(None))
def stage_transformed_incentive_laws(incentive_laws):
    incentive_laws.to_csv(
        "staging/transformed_incentive_laws.csv",
        sep="\t",
        index=False
    )

import pandas.io.sql as sqlio
from sqlalchemy import create_engine, text, exc
from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource
from bokeh.transform import factor_cmap
from bokeh.palettes import Category10
from dagster import op, In, get_dagster_logger
from bokeh.palettes import viridis

postgres_connection_string = "postgresql://postgres:dap@localhost:5432/alternative_fuel_stations"
@op(
    ins={"start": In(bool)}
)
def visualise(start):
    query_string1 = """select light_duty_fuel_configuration_name, count(*) from public."public.light_duty_vehicle" group by light_duty_fuel_configuration_name order by count(*) desc"""
    query_string2 = """select model_year, count(*) from public."public.light_duty_vehicle" group by model_year order by model_year desc"""
    query_string3 = """select "Fuel Type Code", count(*) from public."public.alt_fuel_stations" group by "Fuel Type Code" order by count(*) desc"""
    query_string4 = """select "State", count(*) from public."public.alt_fuel_stations" group by "State" order by count(*) desc"""
    query_string5 = """select technologies, count(*) from public."public.incentive_laws" group by technologies order by count desc LIMIT 10;"""

    logger = get_dagster_logger()

    try:
        engine = create_engine(postgres_connection_string)
        # Distribution of Light Duty Fuel Names
        with engine.connect() as connection:
            alt_fuel_dataframe = sqlio.read_sql_query(
                text(query_string1),
                connection
            )
        p = figure(x_range=alt_fuel_dataframe["light_duty_fuel_configuration_name"].unique(), height=350, title="Distribution of Light Duty Fuel Names",
                   toolbar_location=None, tools="")
        source = ColumnDataSource(data=dict(light_duty_fuel_configuration_name=alt_fuel_dataframe["light_duty_fuel_configuration_name"], count=alt_fuel_dataframe["count"]))
        p.vbar(x='light_duty_fuel_configuration_name', top='count', width=0.9, source=source, line_color="white",
               fill_color=factor_cmap('light_duty_fuel_configuration_name', palette=Category10[10],
                                      factors=alt_fuel_dataframe["light_duty_fuel_configuration_name"].unique()))
        p.xgrid.grid_line_color = None
        p.y_range.start = 0
        p.xaxis.major_label_orientation = 1.0
        p.xaxis.axis_label = "Fuel configuration name"
        p.yaxis.axis_label = "Distribution of charging stations"
        p.title.align = "center"
        p.title.text_font_size = "18px"
        show(p)

        # Distribution of Light Duty Model Years
        with engine.connect() as connection:
            alt_fuel_dataframe = sqlio.read_sql_query(
                text(query_string2),
                connection
            )
        alt_fuel_dataframe['model_year'] = alt_fuel_dataframe['model_year'].astype(str)
        p = figure(x_range=alt_fuel_dataframe["model_year"].unique(), height=350,
                   title="Distribution of Light Duty Model Years",
                   toolbar_location=None, tools="")
        source = ColumnDataSource(
            data=dict(model_year=alt_fuel_dataframe["model_year"], count=alt_fuel_dataframe["count"]))
        p.vbar(x='model_year', top='count', width=0.9, source=source, line_color="white",
               fill_color=factor_cmap('model_year', palette=viridis(len(alt_fuel_dataframe["model_year"].unique())),
                                      factors=alt_fuel_dataframe["model_year"].unique()))
        p.xgrid.grid_line_color = None
        p.y_range.start = 0
        p.xaxis.major_label_orientation = 1.0
        p.xaxis.axis_label = "Year of installation"
        p.yaxis.axis_label = "Station established count"
        p.title.align = "center"
        p.title.text_font_size = "18px"
        show(p)

        # Distribution of Charging Stations by Fuel Type
        with engine.connect() as connection:
            flights_dataframe = sqlio.read_sql_query(
                text(query_string3),
                connection
            )
        fuel_type_code_column = "Fuel Type Code"
        p = figure(x_range=flights_dataframe[fuel_type_code_column].unique(), height=500,
                   title="Distribution of Charging Stations by Fuel Type",
                   toolbar_location=None, tools="")
        source = ColumnDataSource(data=dict(fuel_type_code_column=flights_dataframe[fuel_type_code_column],
                                            count=flights_dataframe["count"]))
        p.vbar(x='fuel_type_code_column', top='count', width=0.9, source=source, line_color="white",
               fill_color=factor_cmap('fuel_type_code_column', palette=Category10[10],
                                      factors=flights_dataframe[fuel_type_code_column].unique()))
        p.xgrid.grid_line_color = None
        p.y_range.start = 0
        p.xaxis.major_label_orientation = 1.0
        p.xaxis.axis_label = "Fuel Types"
        p.yaxis.axis_label = "Charging stations"
        p.title.align = "center"
        p.title.text_font_size = "18px"
        show(p)

        # Charging Stations Adoption by State
        with engine.connect() as connection:
            flights_dataframe = sqlio.read_sql_query(
                text(query_string4),
                connection
            )
        state_column = "State"
        p = figure(x_range=flights_dataframe[state_column].unique(), height=500,
                   title="Charging Stations Adoption by State",
                   toolbar_location=None, tools="", width=950)
        source = ColumnDataSource(data=dict(state_column=flights_dataframe[state_column],
                                            count=flights_dataframe["count"]))
        p.vbar(x='state_column', top='count', width=0.9, source=source, line_color="white",
               fill_color=factor_cmap('state_column', palette=viridis(len(flights_dataframe["State"].unique())),
                                      factors=flights_dataframe[state_column].unique()))
        p.xgrid.grid_line_color = None
        p.y_range.start = 0
        p.xaxis.major_label_orientation = 1.0
        p.xaxis.axis_label = "States"
        p.yaxis.axis_label = "Count of charging stations"
        p.title.align = "center"
        p.title.text_font_size = "18px"
        show(p)

        # Top 10 Combinations of Fuel Types and Technology Categories
        with engine.connect() as connection:
            flights_dataframe = sqlio.read_sql_query(
                text(query_string5),
                connection
            )
        p = figure(x_range=flights_dataframe["technologies"].unique(), height=500,
                   title="Top 10 Combinations of Fuel Types and Technology Categories",
                   toolbar_location=None, tools="", width=950)
        source = ColumnDataSource(data=dict(technologies=flights_dataframe["technologies"],
                                            count=flights_dataframe["count"]))
        p.vbar(x='technologies', top='count', width=0.9, source=source, line_color="white",
               fill_color=factor_cmap('technologies', palette=viridis(len(flights_dataframe["technologies"].unique())),
                                      factors=flights_dataframe["technologies"].unique()))
        p.xgrid.grid_line_color = None
        p.y_range.start = 0
        p.xaxis.major_label_orientation = 1.0
        p.xaxis.axis_label = "Fuel Types and Technology Categories"
        p.yaxis.axis_label = "Count"
        p.title.align = "center"
        p.title.text_font_size = "18px"
        show(p)
    except exc.SQLAlchemyError as dbError:
        logger.error("PostgreSQL Error", dbError)
    finally:
        if engine in locals():
            engine.close()
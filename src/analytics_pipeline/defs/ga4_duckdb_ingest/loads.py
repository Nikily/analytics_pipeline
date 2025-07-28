
import dlt
from .google_analytics import google_analytics

my_load_source = google_analytics()
my_load_pipeline = dlt.pipeline(
    pipeline_name   = "ga4_to_duckdb",
    destination     = 'duckdb',
    dataset_name    = 'ga4_data'
)

# @dlt.source
# def my_source():
#     @dlt.resource
#     def hello_world():
#         yield "hello, world!"

#     return hello_world

# my_load_source = my_source()
# my_load_pipeline = dlt.pipeline(destination="duckdb")

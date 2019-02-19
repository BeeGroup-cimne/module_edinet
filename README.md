# EDINET Data Analysis
Project to execute the data analysis required for the Edinet and similar projects.

## Modules structure.
The project is structured in three different phases of modules:

### 1. Data gathering
Used to obtain the data from different resources and upload it to HBase
- [edinet_billing_measures_etl](edinet_billing_measures_etl/README.md)
- [edinet_metering_measures_etl](edinet_metering_measures_etl/README.md)
- [edinet_meteo_measures](edinet_meteo_input_etl/README.md)

### Data cleaning and errors detection
Used to clean and detect the errors on the uploaded data.
- [edinet_clean_daily](edinet_clean_daily_data_etl/README.md)
- [edinet_clean_hourly](edinet_clean_hourly_data_etl/README.md)
- [edinet_clean_meteo](edinet_clean_meteo_data_etl/README.md)

### Analytics
- [edinet_baseline_hourly](edinet_baseline_hourly_module/README.md)
- [edinet_baseline_monthly](edinet_baseline_monthly_module/README.md)
- [edinet_comparison](edinet_comparison_module/README.md)


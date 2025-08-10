import pandas as pd
from rich import print

df = pd.read_csv("data/new_linkedin_sdr_data_Thu Aug 07 2025 05_04_03 GMT+0000 (Coordinated Universal Time).csv")

remove_columns = ["posterProfileUrl", "posterFullName", "descriptionHtml", "companyId", "workType", "experienceLevel", 'applicationsCount', 'applyType', 'applyUrl', 'benefits', 'publishedAt', 'id']


df.drop(remove_columns, axis =1, inplace=True, errors="ignore")

# Keep track of all columns to drop after merging
# cols_to_drop = set()

# merge_column_names = {
#     'location': ['location/city', 'location/country', 'location/countryCode', 'location/formattedAddressLong', 'location/fullAddress', 'location/postalCode', 'location/streetAddress'],
#     'salary_info': ['salary/salaryCurrency', 'salary/salaryMax', 'salary/salaryMin', 'salary/salaryText', 'salary/salaryType']
# }

# for new_col, cols in merge_column_names.items():
#     # Build dict-like string for each row
#     def row_to_dict(row):
#         non_nulls = {col: row[col] for col in cols if pd.notna(row[col])}
#         return str(non_nulls)
#
#     df[new_col] = df.apply(row_to_dict, axis=1)
#     cols_to_drop.update(cols)
#
#
# # Drop the original columns used for merging
# df = df.drop(columns=list(cols_to_drop), errors="ignore")


rename_mapping = {
    'title': "job_title",
    'companyName': "company_name",
    'location': "company_location",
    'postedTime': "post_date",
    'contractType': "job_type",
    'description': "job_description",
    'salary': "salary",
    'jobUrl': "job_url",
    'companyUrl': "company_website",
    'sector': "industry"}

df.rename(columns=rename_mapping, inplace=True)


new_column_order = [ "company_name", "company_location", "job_title", "job_description",  "job_type", "salary", "job_url", "company_website", "industry", "post_date" ]

df_custom_sorted = df[new_column_order]

print(df_custom_sorted.columns.values)
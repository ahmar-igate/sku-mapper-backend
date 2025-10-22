from datetime import datetime
import io
import logging
from rest_framework_simplejwt.views import TokenObtainPairView #type: ignore
from rest_framework.permissions import AllowAny #type: ignore
from rest_framework.views import APIView #type: ignore
from django.db import DatabaseError #type: ignore
from rest_framework import status #type: ignore
from rest_framework.response import Response #type: ignore
from django.db import connections #type: ignore
from .serializers import CustomTokenObtainPairSerializer, NewProductMappingSerializer, ProductMappingSerializer
from .models import product_mapping, new_product_mapping
import pandas as pd #type: ignore
from django.db.models import Q
from django.http import JsonResponse
from django.db import transaction #type: ignore
import numpy as np
logger = logging.getLogger(__name__)

# Function to update im_sku values within a group
def update_im_sku(group):
    # Remove empty strings and NaN values from im_sku for checking
    non_empty = group['im_sku'].replace('', np.nan).dropna()
    if non_empty.empty:
        # Nothing to update if all im_sku values are empty
        return group
    
    # Function to extract base sku (strip trailing '+' if present)
    def base_sku(x):
        return x[:-1] if x.endswith('+') else x

    # Create a temporary column with the base im_sku
    group['base_im_sku'] = group['im_sku'].apply(lambda x: base_sku(x) if isinstance(x, str) and x != '' else x)
    
    # Get unique non-empty base values from the group
    unique_bases = group.loc[group['base_im_sku'].notnull(), 'base_im_sku'].unique()
    
    # We expect that for a given ASIN there is one "similar" im_sku
    if len(unique_bases) == 1:
        base_val = unique_bases[0]
        # Check if any row in this group already has a plus appended
        plus_present = group['im_sku'].apply(lambda x: isinstance(x, str) and x.endswith('+')).any()
        # Determine desired im_sku for the group
        desired = base_val + '+' if plus_present else base_val
        
        # Update rows:
        # - If im_sku is empty, fill it with the desired im_sku.
        # - If im_sku equals the base (i.e. missing the plus) but desired includes a plus, update it.
        group['im_sku'] = group['im_sku'].apply(
            lambda x: desired if (not isinstance(x, str)) or x == '' or x == base_val else x
        )
    
    # Remove the temporary column
    group = group.drop(columns=['base_im_sku'])
    return group

def fill_parent_sku_base_on_im_sku(df):
    # Strip whitespace from existing non-null parent_sku values
    df['parent_sku'] = df['parent_sku'].apply(lambda x: x.strip() if isinstance(x, str) else x)
    
    # Fill missing parent_sku values within each im_sku group
    df['parent_sku'] = df.groupby('im_sku')['parent_sku'].transform(lambda x: x.ffill().bfill())
    return df

class CustomTokenObtainPairView(TokenObtainPairView):
    # Allow any user (authenticated or not) to access this view
    permission_classes = (AllowAny,)
    serializer_class = CustomTokenObtainPairSerializer
    
# def import_product_mapping(request):
#     if request.method == "GET":
#         try:
#             # Read CSV file
#             file_path = "D:/igate/Sku mapper/sku-mapper-b/app/product_mapping.csv"
#             data = pd.read_csv(file_path)

#             # Prepare list for bulk_create
#             records = []
#             for _, row in data.iterrows():
#                 record = product_mapping(
#                     marketplace_sku=row["marketplace_sku"],
#                     asin=row["asin"],
#                     im_sku=row["im_sku"],
#                     region=row["region"],
#                     sales_channel=row["SalesChannel"],
#                     level_1=row["level_1"],
#                     linworks_title=row["Linnworks Title"],
#                     # parent_sku=row["parent_sku"] if "parent_sku" in row and pd.notna(row["parent_sku"]) else None,
#                     modified_by=row["linnwork's_sku_received_from"],
#                     comment=row["Comment"] if "Comment" in row and pd.notna(row["Comment"]) else None,
#                 )
#                 records.append(record)

#             # Bulk create for performance optimization
#             product_mapping.objects.bulk_create(records, ignore_conflicts=True)

#             return JsonResponse({"message": "Data imported successfully"}, status=201)

#         except Exception as e:
#             return JsonResponse({"error": str(e)}, status=500)

#     return JsonResponse({"error": "Only POST method allowed"}, status=405)


def import_product_mapping(request):
    if request.method == "GET":
        try:
            # Step 1: Import CSV data into product_mapping table.
            file_path = "D:/igate/Sku Mapper/sku-mapper-backend/app/product_mapping.csv"
            data = pd.read_csv(file_path)
            records = []
            for _, row in data.iterrows():
                record = product_mapping(
                    marketplace_sku=row["marketplace_sku"],
                    asin=row["asin"],
                    im_sku=row["im_sku"],
                    region=row["region"],
                    sales_channel=row["SalesChannel"],
                    level_1=row["level_1"],
                    linworks_title=row["Linnworks Title"],
                    modified_by=row["linnwork's_sku_received_from"],
                    comment=row["Comment"] if "Comment" in row and pd.notna(row["Comment"]) else None,
                )
                records.append(record)
            product_mapping.objects.bulk_create(records, ignore_conflicts=True)

            # Step 2: Execute the join query on the secondary database.
            query = """
WITH CombinedData AS (
    SELECT
        UPPER(LTRIM(RTRIM(SellerSKU_Optimized))) AS SellerSKU,
        ASIN,
        Region,
        UPPER(LTRIM(RTRIM(SalesChannel_Optimized))) AS SalesChannel,
        PurchaseDate_Materialized AS PurchaseDate,
        Title
    FROM dbo.amazon_api_de
    WHERE OrderStatus_Optimized = 'Shipped' 
      AND UPPER(LTRIM(RTRIM(SalesChannel_Optimized))) <> 'NON-AMAZON'
    
    UNION ALL
    
    SELECT
        UPPER(LTRIM(RTRIM(SellerSKU_Optimized))),
        ASIN,
        Region,
        UPPER(LTRIM(RTRIM(SalesChannel_Optimized))),
        PurchaseDate_Materialized,
        Title
    FROM dbo.amazon_api_es
    WHERE OrderStatus_Optimized = 'Shipped' 
      AND UPPER(LTRIM(RTRIM(SalesChannel_Optimized))) <> 'NON-AMAZON'
    
    UNION ALL
    
    SELECT
        UPPER(LTRIM(RTRIM(SellerSKU_Optimized))),
        ASIN,
        Region,
        UPPER(LTRIM(RTRIM(SalesChannel_Optimized))),
        PurchaseDate_Materialized,
        Title
    FROM dbo.amazon_api_it
    WHERE OrderStatus_Optimized = 'Shipped' 
      AND UPPER(LTRIM(RTRIM(SalesChannel_Optimized))) <> 'NON-AMAZON'
    
    UNION ALL
    
    SELECT
        UPPER(LTRIM(RTRIM(SellerSKU_Optimized))),
        ASIN,
        Region,
        UPPER(LTRIM(RTRIM(SalesChannel_Optimized))),
        PurchaseDate_Materialized,
        Title
    FROM dbo.amazon_api_uk
    WHERE OrderStatus_Optimized = 'Shipped' 
      AND UPPER(LTRIM(RTRIM(SalesChannel_Optimized))) <> 'NON-AMAZON'
),
DistinctSellers AS (
    SELECT DISTINCT SellerSKU, ASIN, Region, SalesChannel
    FROM CombinedData
),
LatestTitle AS (
    SELECT
        SellerSKU,
        SalesChannel,
        PurchaseDate,
        Title,
        ROW_NUMBER() OVER (
            PARTITION BY SellerSKU, SalesChannel 
            ORDER BY PurchaseDate DESC
        ) AS rn
    FROM CombinedData
)
SELECT 
    ds.SellerSKU, 
    ds.ASIN, 
    ds.Region, 
    ds.SalesChannel,
    lt.PurchaseDate AS [Date], 
    lt.Title
FROM DistinctSellers ds
LEFT JOIN LatestTitle lt
    ON ds.SellerSKU = lt.SellerSKU 
    AND ds.SalesChannel = lt.SalesChannel
    AND lt.rn = 1;
            """
            with connections['secondary'].cursor() as cursor:
                cursor.execute(query)
                join_results = cursor.fetchall()

            # Step 3: Update matching product_mapping records or create new ones.
            records_to_update = []
            new_records = []
            for row in join_results:
                seller_sku, asin, region, sales_channel, date_val, title = row
                qs = product_mapping.objects.filter(
                    marketplace_sku__iexact=seller_sku,
                    sales_channel__iexact=sales_channel
                )
                if qs.exists():
                    for pm_obj in qs:
                        pm_obj.date = date_val  # Update the date field.
                        pm_obj.amazon_title = title  # Update amazon_title with Title.
                        records_to_update.append(pm_obj)
                # else:
                #     # Create a new record if none exists. Adjust required fields as needed.
                #     new_obj = product_mapping(
                #         marketplace_sku=seller_sku,
                #         asin=asin,
                #         region=region,
                #         sales_channel=sales_channel,
                #         date=date_val,
                #         amazon_title=title,
                #     )
                #     new_records.append(new_obj)

            if records_to_update:
                product_mapping.objects.bulk_update(records_to_update, ['date', 'amazon_title'])
                # product_mapping.objects.bulk_update(records_to_update, ['date', 'amazon_title'])
            # if new_records:
            #     product_mapping.objects.bulk_create(new_records)

            return JsonResponse({"message": "Data imported and joined data saved successfully"}, status=201)

        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)

    return JsonResponse({"error": "Only GET method allowed"}, status=405)



class Dashboard(APIView):
    """
    A protected endpoint example.
    Only accessible with a valid JWT.
    """
    def get(self, request, *args, **kwargs):
        try:
            # Retrieve all mapping data.
            mapping_data_qs = product_mapping.objects.using('default').all()

            # Serialize the queryset
            serializer = ProductMappingSerializer(mapping_data_qs, many=True)
            # print(type(serializer.data))
            df = pd.DataFrame(serializer.data)
            null_im_sku = df['im_sku'].isnull().sum()
            unique_im_sku = df['im_sku'].str.strip().nunique()
            unique_parent_sku = df['parent_sku'].str.strip().nunique()
            unique_marketplace_sku = df['marketplace_sku'].nunique()
            unique_regions = df['region'].nunique()
            null_parent_sku = df['parent_sku'].isnull().sum()
            lin_category_to_be_mapped = df['level_1'].isnull().sum()
            lin_title_to_be_mapped = df['linworks_title'].isnull().sum()
            # Filter rows where category is 'Abondedn items' (case and space insensitive)
            abandoned_df = df[df['level_1'].str.strip().str.lower() == 'abandoned items']
            # Count unique im_sku values after stripping whitespace
            unique_im_sku_count_hvng_abondend_items = abandoned_df['im_sku'].str.strip().nunique()

            
        except DatabaseError as db_err:
            # Log database-related errors
            logger.error("Database error when fetching product mappings: %s", db_err, exc_info=True)
            return Response(
                {"error": "A database error occurred while retrieving product mapping data."},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        except Exception as e:
            # Log any unexpected errors
            logger.error("Unexpected error when fetching product mappings: %s", e, exc_info=True)
            return Response(
                {"error": "An unexpected error occurred."},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        
        # Return the serialized data along with a welcome message
        return Response(
            {
                "message": "Welcome to the dashboard!!!",
                "mapping_data": serializer.data,
                "null_im_sku": null_im_sku,
                "unique_im_sku": unique_im_sku,
                "unique_marketplace_sku": unique_marketplace_sku,
                "unique_parent_sku": unique_parent_sku,
                "unique_regions": unique_regions,
                "null_parent_sku": null_parent_sku,
                "lin_category_to_be_mapped": lin_category_to_be_mapped,
                "lin_title_to_be_mapped": lin_title_to_be_mapped,
                "unique_im_sku_hvng_abondoned_items": unique_im_sku_count_hvng_abondend_items,
            },
            status=status.HTTP_200_OK
        )
def update_lin_categ_title_if_exists(df):
    print("🔄 Starting update of level_1 and linworks_title based on im_sku...")

    # Step 0: Clean im_sku values
    df['im_sku'] = df['im_sku'].astype(str).str.strip()
    print("✅ Cleaned im_sku values.")

    # Step 1: Filter rows with valid data
    valid_rows = df[
        df['im_sku'].notna() & (df['im_sku'].str.strip() != '') &
        df['level_1'].notna() & (df['level_1'].str.strip() != '') &
        df['linworks_title'].notna() & (df['linworks_title'].str.strip() != '')
    ]
    print(f"✅ Found {len(valid_rows)} valid rows with complete im_sku, level_1, and linworks_title.")

    # Step 2: Group and create mapping
    valid_im_skus = valid_rows.groupby('im_sku').agg({
        'level_1': 'first',
        'linworks_title': 'first'
    }).reset_index()

    print("🔍 Valid im_sku mappings:")
    print(valid_im_skus)

    im_sku_to_category = dict(zip(valid_im_skus['im_sku'], valid_im_skus['level_1']))
    im_sku_to_title = dict(zip(valid_im_skus['im_sku'], valid_im_skus['linworks_title']))

    # Step 3: Fill missing values
    def fill_level_1(row):
        if pd.isna(row['level_1']) or str(row['level_1']).strip() == '':
            filled = im_sku_to_category.get(row['im_sku'], row['level_1'])
            if filled != row['level_1']:
                print(f"✏️ Filling level_1 for im_sku '{row['im_sku']}' with '{filled}'")
            return filled
        return row['level_1']

    def fill_linworks_title(row):
        if pd.isna(row['linworks_title']) or str(row['linworks_title']).strip() == '':
            filled = im_sku_to_title.get(row['im_sku'], row['linworks_title'])
            if filled != row['linworks_title']:
                print(f"✏️ Filling linworks_title for im_sku '{row['im_sku']}' with '{filled}'")
            return filled
        return row['linworks_title']

    df['level_1'] = df.apply(fill_level_1, axis=1)
    df['linworks_title'] = df.apply(fill_linworks_title, axis=1)

    print("✅ Update completed.\n")
    return df
    
        
class New_Mapping(APIView):
    def get(self, request, *args, **kwargs):
        try:
            # 1. Retrieve product mapping data from the default database including all desired columns.
            mapping_qs = product_mapping.objects.using('default').filter(im_sku__isnull=False).exclude(im_sku='')
            mapping_data = list(mapping_qs.values(
                "marketplace_sku",  # join key
                "asin",             # join key
                "region",           # join key
                "im_sku",           # additional field
                "sales_channel",    # additional field
                "level_1",          # additional field
                "parent_sku",     # additional field
                "linworks_title",   # additional field
                "modified_by",      # additional field
                "comment",           # additional field
                "date"   ,           # additional field
                "amazon_title" ,     # additional field
            ))
            
            # 2. Retrieve Amazon data from the secondary database.
            # query = """
            # select
            #     distinct SellerSKU, ASIN, Region, SalesChannel
            #     from(
            #             select SellerSKU, ASIN, Region, SalesChannel from dbo.amazon_api_de
            #             where OrderStatus = 'Shipped' and SalesChannel != 'Non-Amazon'
            #             union 
            #             select SellerSKU, ASIN, Region, SalesChannel from dbo.amazon_api_es
            #             where OrderStatus = 'Shipped' and SalesChannel != 'Non-Amazon'
            #             union 
            #             select SellerSKU, ASIN, Region, SalesChannel from dbo.amazon_api_it
            #             where OrderStatus = 'Shipped' and SalesChannel != 'Non-Amazon'
            #             union 
            #             select SellerSKU, ASIN, Region, SalesChannel from dbo.amazon_api_uk
            #             where OrderStatus = 'Shipped' and SalesChannel != 'Non-Amazon'
            #     ) as a;
            # """
            query = """
WITH CombinedData AS (
    SELECT
        UPPER(LTRIM(RTRIM(SellerSKU_Optimized))) AS SellerSKU,
        ASIN,
        Region,
        UPPER(LTRIM(RTRIM(SalesChannel_Optimized))) AS SalesChannel,
        PurchaseDate_Materialized AS PurchaseDate,
        Title
    FROM dbo.amazon_api_de
    WHERE OrderStatus_Optimized = 'Shipped' 
      AND UPPER(LTRIM(RTRIM(SalesChannel_Optimized))) <> 'NON-AMAZON'
    
    UNION ALL
    
    SELECT
        UPPER(LTRIM(RTRIM(SellerSKU_Optimized))),
        ASIN,
        Region,
        UPPER(LTRIM(RTRIM(SalesChannel_Optimized))),
        PurchaseDate_Materialized,
        Title
    FROM dbo.amazon_api_es
    WHERE OrderStatus_Optimized = 'Shipped' 
      AND UPPER(LTRIM(RTRIM(SalesChannel_Optimized))) <> 'NON-AMAZON'
    
    UNION ALL
    
    SELECT
        UPPER(LTRIM(RTRIM(SellerSKU_Optimized))),
        ASIN,
        Region,
        UPPER(LTRIM(RTRIM(SalesChannel_Optimized))),
        PurchaseDate_Materialized,
        Title
    FROM dbo.amazon_api_it
    WHERE OrderStatus_Optimized = 'Shipped' 
      AND UPPER(LTRIM(RTRIM(SalesChannel_Optimized))) <> 'NON-AMAZON'
    
    UNION ALL
    
    SELECT
        UPPER(LTRIM(RTRIM(SellerSKU_Optimized))),
        ASIN,
        Region,
        UPPER(LTRIM(RTRIM(SalesChannel_Optimized))),
        PurchaseDate_Materialized,
        Title
    FROM dbo.amazon_api_uk
    WHERE OrderStatus_Optimized = 'Shipped' 
      AND UPPER(LTRIM(RTRIM(SalesChannel_Optimized))) <> 'NON-AMAZON'
),
DistinctSellers AS (
    SELECT DISTINCT SellerSKU, ASIN, Region, SalesChannel
    FROM CombinedData
),
LatestTitle AS (
    SELECT
        SellerSKU,
        SalesChannel,
        PurchaseDate,
        Title,
        ROW_NUMBER() OVER (
            PARTITION BY SellerSKU, SalesChannel 
            ORDER BY PurchaseDate DESC
        ) AS rn
    FROM CombinedData
)
SELECT 
    ds.SellerSKU, 
    ds.ASIN, 
    ds.Region, 
    ds.SalesChannel,
    lt.PurchaseDate AS [Date], 
    lt.Title
FROM DistinctSellers ds
LEFT JOIN LatestTitle lt
    ON ds.SellerSKU = lt.SellerSKU 
    AND ds.SalesChannel = lt.SalesChannel
    AND lt.rn = 1;
            """
            with connections['secondary'].cursor() as cursor:
                cursor.execute(query)
                amazon_results = cursor.fetchall()
            # Convert the Amazon results (list of tuples) into a list of dictionaries.
            amazon_data = [
                {"SellerSKU": row[0], "ASIN": row[1], "Region": row[2], "SalesChannel": row[3], "Date": row[4], "Title": row[5]} 
                for row in amazon_results
            ]
            print("amazon_data: ", amazon_data[12])
            
            # --- Transformation Step on Secondary DB Records ---
            # Convert to DataFrame and apply transformations on the 'SellerSKU' column.
            df_amazon = pd.DataFrame(amazon_data)
            # df_amazon = update_lin_categ_title_if_exists(df_amazon)
            df_amazon.to_csv("amazon_data.csv", encoding='utf-8', index=False)
            if not df_amazon.empty:
                df_amazon['SellerSKU'] = df_amazon['SellerSKU'].astype(str)
                df_amazon['SellerSKU'] = df_amazon['SellerSKU'].apply(lambda x: x.strip())
                df_amazon['SellerSKU'] = df_amazon['SellerSKU'].apply(str.upper)
            # Convert back to a list of dictionaries.
            amazon_data = df_amazon.to_dict('records')
            # --- End Transformation Step ---

            # 3. Build a lookup dictionary for product mapping keyed by the full join key (marketplace_sku, asin, region)
            mapping_lookup = {
                (record["marketplace_sku"], record["asin"], record["region"]): record
                for record in mapping_data
            }
            
            # 4. Perform a right join by iterating over the Amazon data.
            joined_data = []
            for amazon_record in amazon_data:
                # Build the join key from the Amazon record.
                key = (amazon_record["SellerSKU"], amazon_record["ASIN"], amazon_record["Region"])
                # Attempt to retrieve a matching product mapping record.
                mapping_record = mapping_lookup.get(key)
                
                # Build the final joined record.
                joined_record = {
                    "marketplace_sku": amazon_record["SellerSKU"],
                    "asin": amazon_record["ASIN"],
                    "region": amazon_record["Region"],
                    "im_sku": mapping_record["im_sku"] if mapping_record else None,
                    "sales_channel": mapping_record["sales_channel"] if mapping_record else amazon_record.get("SalesChannel"),
                    "level_1": mapping_record["level_1"] if mapping_record else None,
                    "parent_sku": mapping_record["parent_sku"] if mapping_record else None,
                    "linworks_title": mapping_record["linworks_title"] if mapping_record else None,
                    "modified_by": mapping_record["modified_by"] if mapping_record else None,
                    "comment": mapping_record["comment"] if mapping_record else None,
                    "date": mapping_record["date"] if mapping_record else amazon_record.get("Date"),
                    "amazon_title": mapping_record["amazon_title"] if mapping_record else amazon_record.get("Title"),
                }
                joined_data.append(joined_record)
            
            # print("Joined data type: ", type(joined_data))
            # print("Joined data: ", joined_data)
            joined_data_df = pd.DataFrame(joined_data)
            # joined_data_df = update_lin_categ_title_if_exists(joined_data_df)
            print("joined_data_df: ", joined_data_df['level_1'].isnull().sum())
            # updated_sku_df = joined_data_df.groupby('asin', group_keys=False).apply(update_im_sku)
            # joined_data = updated_sku_df.to_dict('records')
            # Step 1: Apply update_im_sku first (if needed)
            joined_data_df = joined_data_df.groupby('asin', group_keys=False).apply(update_im_sku)
            
            # Step 2: Then apply enrichment for level_1 and linworks_title
            joined_data_df = update_lin_categ_title_if_exists(joined_data_df)
            print("joined_data_df: ", joined_data_df['level_1'].isnull().sum())
            
            
            # Debug: Print a few examples of rows with non-null level_1 values after update
            print("Sample updated level_1 values:")
            sample_rows = joined_data_df[joined_data_df['level_1'].notna()].head(3)
            for _, row in sample_rows.iterrows():
                print(f"ASIN: {row['asin']}, SKU: {row['marketplace_sku']}, level_1: {row['level_1']}")
                
            joined_data_df = fill_parent_sku_base_on_im_sku(joined_data_df)
            
            # Step 3: Convert to dict
            joined_data = joined_data_df.to_dict('records')
            
            mapping_lookup = {
                (record["marketplace_sku"], record["region"]): record
                for record in mapping_data
                if record["im_sku"] is not None
            }

            for record in joined_data:
                sku = record["marketplace_sku"]
                region = record["region"]

                # First, lookup using the SKU as is.
                mapping_record = mapping_lookup.get((sku, region))
                if mapping_record:
                    record["im_sku"] = mapping_record.get("im_sku").strip() if mapping_record.get("im_sku") else mapping_record.get("im_sku")
                    record["sales_channel"] = mapping_record.get("sales_channel")
                    record["level_1"] = mapping_record.get("level_1").strip() if mapping_record.get("level_1") else mapping_record.get("level_1")
                    record["linworks_title"] = mapping_record.get("linworks_title").strip() if mapping_record.get("linworks_title") else mapping_record.get("linworks_title")
                    record["parent_sku"] = mapping_record.get("parent_sku").strip() if mapping_record.get("parent_sku") else mapping_record.get("parent_sku")
                    record["modified_by"] = mapping_record.get("modified_by")
                    record["comment"] = mapping_record.get("comment")

                # Then, form the alternate SKU: if it starts with "M-", remove it; otherwise, add "M-"
                if sku.startswith("M-"):
                    alternate_sku = sku[2:]
                else:
                    alternate_sku = "M-" + sku

                # Lookup using the alternate SKU
                mapping_record_alt = mapping_lookup.get((alternate_sku, region))
                if mapping_record_alt:
                    # Overwrite mapping fields with the alternate record if found
                    record["im_sku"] = mapping_record_alt.get("im_sku").strip() if mapping_record_alt.get("im_sku") else mapping_record_alt.get("im_sku")
                    record["sales_channel"] = mapping_record_alt.get("sales_channel")
                    record["level_1"] = mapping_record_alt.get("level_1").strip() if mapping_record_alt.get("level_1") else mapping_record_alt.get("level_1")
                    record["parent_sku"] = mapping_record_alt.get("parent_sku").strip() if mapping_record_alt.get("parent_sku") else mapping_record_alt.get("parent_sku")
                    record["linworks_title"] = mapping_record_alt.get("linworks_title").strip() if mapping_record_alt.get("linworks_title") else mapping_record_alt.get("linworks_title")
                    record["modified_by"] = mapping_record_alt.get("modified_by")
                    record["comment"] = mapping_record_alt.get("comment")
            
            # 6. Export the joined data to CSV using pandas.
            # df = pd.DataFrame(joined_data)
            # df.to_csv("joined_data.csv", encoding='utf-8', index=False)
            
            # 7. Save the joined data to the product_mapping table in the default database.
            # Use the composite key (marketplace_sku, asin, region) to avoid duplicate records.
            join_keys = [(rec["marketplace_sku"], rec["asin"], rec["region"]) for rec in joined_data]
            # Get unique values for filtering.
            unique_marketplace_skus = {key[0] for key in join_keys}
            unique_asins = {key[1] for key in join_keys}
            unique_regions = {key[2] for key in join_keys}
            # unique_im_sku = {key[3] for key in join_keys}
            
            # Query for existing records in the default DB that match these keys.
            existing_objs = product_mapping.objects.using('default').filter(
                marketplace_sku__in=unique_marketplace_skus,
                asin__in=unique_asins,
                region__in=unique_regions,
                # im_sku__in=unique_im_sku,
            )
            # Build a lookup dict using the composite key.
            existing_map = {
                (obj.marketplace_sku, obj.asin, obj.region): obj 
                for obj in existing_objs
            }
            
            # Prepare lists for bulk update and bulk create.
            objs_to_update = []
            objs_to_create = []
            
            # Debug counters
            update_count = 0
            create_count = 0
            level1_null_before = 0
            level1_not_null_before = 0
            
            for record in joined_data:
                key = (record["marketplace_sku"], record["asin"], record["region"])
                if key in existing_map:
                    obj = existing_map[key]
                    
                    # Debug: Check level_1 values before update
                    if obj.level_1 is None or obj.level_1.strip() == "":
                        level1_null_before += 1
                    else:
                        level1_not_null_before += 1
                    
                    # Update fields
                    obj.im_sku = record["im_sku"]
                    obj.sales_channel = record["sales_channel"]
                    obj.level_1 = record["level_1"]
                    obj.linworks_title = record["linworks_title"]
                    obj.modified_by = record["modified_by"]
                    obj.parent_sku = record["parent_sku"]
                    obj.comment = record["comment"]
                    obj.date = record["date"]
                    obj.amazon_title = record["amazon_title"]
                    objs_to_update.append(obj)
                    update_count += 1
                else:
                    new_obj = product_mapping(
                        marketplace_sku=record["marketplace_sku"],
                        asin=record["asin"],
                        region=record["region"],
                        im_sku=record["im_sku"],
                        sales_channel=record["sales_channel"],
                        level_1=record["level_1"],
                        parent_sku=record["parent_sku"],
                        linworks_title=record["linworks_title"],
                        modified_by=record["modified_by"],
                        comment=record["comment"],
                        date = record["date"],
                        amazon_title = record["amazon_title"]
                    )
                    objs_to_create.append(new_obj)
                    create_count += 1
            
            print(f"Processing summary - Update: {update_count}, Create: {create_count}")
            print(f"Before update - Null level_1: {level1_null_before}, Not null level_1: {level1_not_null_before}")
            
            # Use a transaction to ensure atomicity.
            with transaction.atomic(using='default'):
                if objs_to_update:
                    print(f"Bulk updating {len(objs_to_update)} records...")
                    product_mapping.objects.using('default').bulk_update(
                        objs_to_update,
                        ['sales_channel', 'level_1', 'linworks_title', 'modified_by', 'comment', 'im_sku', 'date', 'amazon_title', 'parent_sku'],
                    )
                if objs_to_create:
                    print(f"Bulk creating {len(objs_to_create)} records...")
                    product_mapping.objects.using('default').bulk_create(objs_to_create)
            
            # Verify the updates were applied
            if objs_to_update:
                updated_records = product_mapping.objects.using('default').filter(
                    marketplace_sku__in=[obj.marketplace_sku for obj in objs_to_update[:5]]
                )
                print("Sample updated records after save:")
                for rec in updated_records[:3]:
                    print(f"ASIN: {rec.asin}, SKU: {rec.marketplace_sku}, level_1: {rec.level_1}")
            
            return Response(
                {"message": "success"},
                status=status.HTTP_200_OK
            )
            
        except DatabaseError as db_err:
            logger.error("Database error when loading new mapping: %s", db_err, exc_info=True)
            return Response(
                {"error": "A database error occurred."},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        except Exception as e:
            logger.error("Unexpected error when loading new mapping: %s", e, exc_info=True)
            return Response(
                {"error": "An unexpected error occurred."},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
            

class UpdateMapping(APIView):
    def put(self, request, id, *args, **kwargs):
        try:
            mapping_data = request.data
            # Disallow updates for read-only users
            dept = request.data.get('department') if isinstance(request.data, dict) else None
            if dept and str(dept).upper() == 'READ_ONLY':
                return Response({'message': 'Read-only users cannot update mappings.'}, status=status.HTTP_403_FORBIDDEN)
            logger.info("Received mapping data for id %s: %s", id, mapping_data)
            print("Received mapping data for id %s: %s", id, mapping_data)
            response_data, created = updateMapping_helper(mapping_data, id)

            status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
            return Response(response_data, status=status_code)
        except Exception as e:
            logger.error("Unexpected error when saving mapping: %s", e, exc_info=True)
            return Response(
                {"error": "An unexpected error occurred."},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

def updateMapping_helper(mapping_data, id):
    
    # ------------------------------------------------------------------
    # STEP 1: Update or create product_mapping record
    # ------------------------------------------------------------------
    # Handle parent_sku - allow it to be set to null/empty
    incoming_parent_sku = mapping_data.get('parent_sku')
    if incoming_parent_sku is not None:
        incoming_parent_sku = incoming_parent_sku.strip() if incoming_parent_sku else None
    obj, created = product_mapping.objects.update_or_create(
        id=id,
        defaults={
            'marketplace_sku': mapping_data.get('marketplace_sku', '').strip() if mapping_data.get('marketplace_sku') else None,
            'asin': mapping_data.get('asin', '').strip() if mapping_data.get('asin') else None,
            'im_sku': mapping_data.get('im_sku', '').strip() if mapping_data.get('im_sku') else None,
            'parent_sku': incoming_parent_sku.strip() if incoming_parent_sku else None,
            'region': mapping_data.get('region', '').strip() if mapping_data.get('region') else None,
            'sales_channel': mapping_data.get('sales_channel', '').strip() if mapping_data.get('sales_channel') else None,
            'level_1': mapping_data.get('level_1', '').strip() if mapping_data.get('level_1') else None,
            'linworks_title': mapping_data.get('linworks_title', '').strip() if mapping_data.get('linworks_title') else None,
            'comment': mapping_data.get('comment', '').strip() if mapping_data.get('comment') else None,
            'comment_by_finance': mapping_data.get('comment_by_finance', '').strip() if mapping_data.get('comment_by_finance') else None,
        }
    )
    # Update the appropriate modified_by field based on department
    if mapping_data.get('modified_by') and mapping_data.get('modified_by').strip():
        obj.modified_by = mapping_data.get('modified_by')
    if mapping_data.get('modified_by_finance') and mapping_data.get('modified_by_finance').strip():
        obj.modified_by_finance = mapping_data.get('modified_by_finance')
    if mapping_data.get('modified_by_admin') and mapping_data.get('modified_by_admin').strip():
        obj.modified_by_admin = mapping_data.get('modified_by_admin')
    
    obj.save()
    # ------------------------------------------------------------------
    # STEP 2: Fill missing parent_sku for all rows with the same im_sku
    # ------------------------------------------------------------------
    # If there's a valid im_sku
    im_sku_value = (obj.im_sku or '').strip()
    if im_sku_value:
        # Update all records with the same im_sku to have the same parent_sku
        # This will set parent_sku to None if incoming_parent_sku is None
        product_mapping.objects.filter(im_sku__iexact=im_sku_value).update(parent_sku=incoming_parent_sku)
        logger.info(
            "Updated parent_sku=%s for all records with im_sku=%s",
            incoming_parent_sku, im_sku_value
        )
        # Schedule the tertiary database update as a background task
        try:
            from threading import Thread
            def update_tertiary_db():
                try:
                    with transaction.atomic(using='tertiary'):
                        with connections['tertiary'].cursor() as cursor:
                            update_sql = """
                                UPDATE look_product_hierarchy_test
                                SET parent_sku = %s
                                WHERE im_sku = %s
                            """
                            cursor.execute(update_sql, [incoming_parent_sku, im_sku_value])
                            logger.info(
                                "Updated parent_sku=%s for all records with im_sku=%s in tertiary database",
                                incoming_parent_sku, im_sku_value
                            )
                except Exception as e:
                    logger.error(
                        "Error updating parent_sku in tertiary database for im_sku=%s: %s",
                        im_sku_value, e, exc_info=True
                    )
            # Start the background task
            Thread(target=update_tertiary_db).start()
        except Exception as e:
            logger.error(
                "Error scheduling tertiary database update for im_sku=%s: %s",
                im_sku_value, e, exc_info=True
            )
    
    # ------------------------------------------------------------------
    # STEP 2.5: Update appropriate modified_by field for all records with same im_sku
    # ------------------------------------------------------------------
    if im_sku_value:
        # Add debugging to inspect the values
        print("DEBUG - Modified by values received:")
        print(f"modified_by (SCM): {mapping_data.get('modified_by')}")
        print(f"modified_by_finance: {mapping_data.get('modified_by_finance')}")
        print(f"modified_by_admin: {mapping_data.get('modified_by_admin')}")
        
        # Determine which mapped_by field to update based on the source of the update
        if mapping_data.get('modified_by') and mapping_data.get('modified_by').strip():
            # SCM department update
            print("Updating modified_by (SCM) field for all records with im_sku:", im_sku_value)
            product_mapping.objects.filter(im_sku__iexact=im_sku_value).update(
                modified_by=mapping_data.get('modified_by')
            )
        elif mapping_data.get('modified_by_finance') and mapping_data.get('modified_by_finance').strip():
            # Finance department update
            print("Updating modified_by_finance field for all records with im_sku:", im_sku_value)
            product_mapping.objects.filter(im_sku__iexact=im_sku_value).update(
                modified_by_finance=mapping_data.get('modified_by_finance')
            )
        elif mapping_data.get('modified_by_admin') and mapping_data.get('modified_by_admin').strip():
            # Admin department update
            print("Updating modified_by_admin field for all records with im_sku:", im_sku_value)
            product_mapping.objects.filter(im_sku__iexact=im_sku_value).update(
                modified_by_admin=mapping_data.get('modified_by_admin')
            )
        else:
            print("No valid modified_by field found, skipping update for im_sku:", im_sku_value)
    
    # ------------------------------------------------------------------
    # STEP 3: Check if im_sku exists and fill in level_1, linworks_title from a reference
    # ------------------------------------------------------------------
    matching_records = product_mapping.objects.filter(im_sku__iexact=im_sku_value)
    logger.info("Found %d records with im_sku = '%s'", matching_records.count(), im_sku_value)
    for record in matching_records:
        logger.info("Record id=%s | level_1='%s' | linworks_title='%s'",
                    record.id, record.level_1, record.linworks_title)
    # Look for a record (excluding this one) that has both level_1 and linworks_title
    reference_record = matching_records.exclude(id=id).filter(
        Q(level_1__isnull=False) & ~Q(level_1__regex=r'^\s*$'),
        Q(linworks_title__isnull=False) & ~Q(linworks_title__regex=r'^\s*$')
    ).first()
    if reference_record:
        logger.info("Using reference record id=%s to fill missing fields.", reference_record.id)
        updated_fields = {}
        if not obj.level_1 or obj.level_1.strip() == "":
            updated_fields['level_1'] = reference_record.level_1
        if not obj.linworks_title or obj.linworks_title.strip() == "":
            updated_fields['linworks_title'] = reference_record.linworks_title
        if updated_fields:
            for key, value in updated_fields.items():
                setattr(obj, key, value)
            obj.save()
    else:
        logger.warning("No valid reference record found for im_sku = '%s'", im_sku_value)
    # ------------------------------------------------------------------
    # STEP 4: Transformation — update all product_mapping with same ASIN
    # ------------------------------------------------------------------
    asin_value = mapping_data.get('asin')
    
    # Add more debugging
    print("DEBUG - Checking which department is updating ASIN:", asin_value)
    print(f"modified_by value: '{mapping_data.get('modified_by')}'")
    print(f"modified_by_finance value: '{mapping_data.get('modified_by_finance')}'")
    print(f"modified_by_admin value: '{mapping_data.get('modified_by_admin')}'")
    
    # Determine which mapped_by field to update for ASIN records
    if mapping_data.get('modified_by') and mapping_data.get('modified_by').strip():
        print("Updating SCM department data for ASIN:", asin_value)
        # SCM department update
        updated_count = product_mapping.objects.filter(asin=asin_value).update(
            im_sku=mapping_data.get('im_sku'),
            modified_by=mapping_data.get('modified_by')
        )
    elif mapping_data.get('modified_by_finance') and mapping_data.get('modified_by_finance').strip():
        print("Updating Finance department data for ASIN:", asin_value)
        # Finance department update
        updated_count = product_mapping.objects.filter(asin=asin_value).update(
            im_sku=mapping_data.get('im_sku'),
            modified_by_finance=mapping_data.get('modified_by_finance')
        )
    elif mapping_data.get('modified_by_admin') and mapping_data.get('modified_by_admin').strip():
        print("Updating Admin department data for ASIN:", asin_value)
        # Admin department update
        updated_count = product_mapping.objects.filter(asin=asin_value).update(
            im_sku=mapping_data.get('im_sku'),
            modified_by_admin=mapping_data.get('modified_by_admin')
        )
    else:
        print("No valid department detected - using default update for ASIN:", asin_value)
        # Default case - just update im_sku
        updated_count = product_mapping.objects.filter(asin=asin_value).update(
            im_sku=mapping_data.get('im_sku')
        )
    # ------------------------------------------------------------------
    # STEP 5: Determine company based on region
    # ------------------------------------------------------------------
    region = mapping_data.get('region')
    if region in ["IT", "UK", "DE"]:
        company = 'B2fitness'
    elif region == "ES":
        company = 'B2fitness LTD'
    elif region in ["US", "CA"]:
        company = 'brandsinn'
    else:
        company = None  # or whatever default you want
    # ------------------------------------------------------------------
    # STEP 6: Normalize sales_channel
    # ------------------------------------------------------------------
    sales_channel = mapping_data.get('sales_channel')
    if sales_channel == "Amazon.co.uk":
        sales_channel = "Amazon.uk"
    # ------------------------------------------------------------------
    # STEP 7: Update or create new_product_mapping
    # ------------------------------------------------------------------
    obj1, created1 = new_product_mapping.objects.update_or_create(
        id=id,
        defaults={
            'marketplace_sku': obj.marketplace_sku.strip() if obj.marketplace_sku else '',
            'asin': obj.asin.strip() if obj.asin else '',
            'im_sku': obj.im_sku.strip() if obj.im_sku else '',
            'parent_sku': obj.parent_sku.strip() if obj.parent_sku else '',
            'region': obj.region.strip() if obj.region else '',
            'marketplace': sales_channel.strip() if sales_channel else '',
            'level_1': obj.level_1.strip() if obj.level_1 else '',
            'marketplace_sales_table': "stg_tr_amazon_raw",
            'linworks_title': obj.linworks_title.strip() if obj.linworks_title else '',
            'channel': "Amazon",
            'company': company.strip() if isinstance(company, str) else company,
            'modified_by': obj.modified_by.strip() if obj.modified_by else '',
            'modified_by_finance': obj.modified_by_finance.strip() if obj.modified_by_finance else '',
            'modified_by_admin': obj.modified_by_admin.strip() if obj.modified_by_admin else '',
        }
    )
    # ------------------------------------------------------------------
    # STEP 8: Build response data
    # ------------------------------------------------------------------
    if updated_count > 1:
        message = f"{updated_count} ({im_sku_value}) skus have been updated for ASIN ({asin_value}). Refresh your screen to see the changes."
    else:
        message = f"{updated_count} ({im_sku_value}) sku has been updated for ASIN ({asin_value})."
    # Load the latest data from the database to ensure we return accurate values
    obj = product_mapping.objects.get(id=id)
    
    # Log the values we're sending back
    print("DEBUG - Response data fields:")
    print(f"modified_by: {obj.modified_by}")
    print(f"modified_by_finance: {obj.modified_by_finance}")
    print(f"modified_by_admin: {obj.modified_by_admin}")
    
    response_data = {
        'id': obj.id,
        'marketplace_sku': obj.marketplace_sku,
        'asin': obj.asin,
        'im_sku': obj.im_sku,
        'parent_sku': obj.parent_sku,
        'region': obj.region,
        'sales_channel': obj.sales_channel,
        'level_1': obj.level_1,
        'linworks_title': obj.linworks_title,
        'modified_by': obj.modified_by,
        'modified_by_finance': obj.modified_by_finance,
        'modified_by_admin': obj.modified_by_admin,
        'comment': obj.comment,
        'comment_by_finance': obj.comment_by_finance,
        'message': message,
    }
    return response_data, created
   

class BulkUpdateMapping(APIView):
    def post(self, request, *args, **kwargs):
        try:
            # Get the list of mappings from the request body
            req  = request.data
            dept = request.data.get('department')
            print("BulkUpdateMapping request data department: ", dept)
            user_email = request.data.get('user_email')
            print("BulkUpdateMapping request data user_email: ", user_email)
            print("BulkUpdateMapping request data: ", req)
            # Block operation for read-only users
            if dept and str(dept).upper() == 'READ_ONLY':
                return Response({'error': 'Read-only users cannot perform bulk upload.'}, status=403)
            if not dept:
                return Response({'error': 'Unexpected error occured. Department is missing'}, status=400)
            if dept not in ['SCM', 'FINANCE', 'ADMIN']:
                return Response({'error': 'Unexpected error occured. Invalid department'}, status=400)
            if not user_email:
                return Response({'error': 'Unexpected error occured. User email is missing'}, status=400)
            
            uploaded_file = request.FILES.get('file')
            if not uploaded_file:
                return Response({'error': 'No file was uploaded.'}, status=400)

            # Read the file content
            decoded_file = uploaded_file.read().decode('utf-8')
            df = pd.read_csv(io.StringIO(decoded_file))

            print("Parsed CSV DataFrame:")
            print(df.head())  # For debugging
            
            # convert columns to str if except ID
            columns_to_convert = [
                'ASIN', 'Linnworks SKU', 'Linnworks Title', 'Parent SKU',
                'Date', 'Marketplace SKU', 'Region', 'Amazon Title',
                'Sales Channel', 'Linnworks Category', 'Mapped By SCM', 
                'Mapped By Finance', 'Mapped By Admin', 
                'Comment by SCM', 'Comment by Finance'
            ]
            df[columns_to_convert] = df[columns_to_convert].applymap(lambda x: str(x).strip() if pd.notnull(x) else None)

            
            # Validate required columns
            required_columns = {'ID', 'ASIN', 'Linnworks SKU', 'Parent SKU', 'Linnworks Title', 'Date', 'Marketplace SKU', 'Region', 'Amazon Title', 'Sales Channel', 'Linnworks Category'}  # add more as needed
            if not required_columns.issubset(df.columns):
                return Response({'error': f'Missing required columns: {required_columns - set(df.columns)}'},
                                status=400)
            
            # columns to check for null rows
            columns_to_check = [
                'ID', 'ASIN', 'Linnworks SKU', 'Parent SKU', 'Linnworks Title',
                'Date', 'Marketplace SKU', 'Region', 'Amazon Title',
                'Sales Channel', 'Linnworks Category'
            ]
            # Check for NaN, None, or blank ("") values
            has_issues = df[columns_to_check].isnull().any(axis=1) | (df[columns_to_check] == '').any(axis=1)
            # If any problematic rows are found, raise an error
            if has_issues.any():
                return Response({'error': "Data contains null, NaN, or blank ('') values. Please fill it and upload again."}, status=400)
            
            if dept == 'SCM':
                # Overwrite all values in 'Mapped By SCM' with user_email
                df['Mapped By SCM'] = user_email

            elif dept == 'FINANCE':
                # Overwrite all values in 'Mapped By Finance' with user_email
                df['Mapped By Finance'] = user_email
            else:
                df['Mapped By Admin'] = user_email
            
            selected_columns = [
                'ID', 'ASIN', 'Linnworks SKU', 'Linnworks Title', 'Parent SKU',
                'Date', 'Marketplace SKU', 'Region', 'Amazon Title',
                'Sales Channel', 'Linnworks Category', 'Mapped By SCM', 'Mapped By Finance', 'Mapped By Admin', 'Comment by SCM', 'Comment by Finance'
            ]
            df = df[selected_columns]
            df['Parent SKU'] = df['Parent SKU'].astype(str)
            print("Filtered DataFrame with selected columns:")
            print(df.head())  # For debugging
            print(df['Linnworks SKU'].isnull().any())
            print(df['Linnworks Category'].isnull().any())

            


            
            for _, row in df.iterrows():
                # Map CSV column names to the snake_case keys expected by updateMapping_helper
                helper_mapping_data = {
                    'id': row['ID'],
                    'date': row.get('Date'),
                    'marketplace_sku': row.get('Marketplace SKU'),
                    'asin': row.get('ASIN'),
                    'im_sku': row.get('Linnworks SKU'),
                    'parent_sku': row.get('Parent SKU'),
                    'region': row.get('Region'),
                    'sales_channel': row.get('Sales Channel'),
                    'level_1': row.get('Linnworks Category'),
                    'linworks_title': row.get('Linnworks Title'),
                    'amazon_title': row.get('Amazon Title'),
                    'parent_sku': row.get('Parent SKU'),
                    'modified_by': row.get('Mapped By SCM') if dept == 'SCM' else None,
                    'modified_by_finance': row.get('Mapped By Finance') if dept == 'FINANCE' else None,
                    'modified_by_admin': row.get('Mapped By Admin') if dept == 'ADMIN' else None,
                    'comment': row.get('Comment by SCM') if dept == 'SCM' else None,
                    'comment_by_finance': row.get('Comment by Finance') if dept == 'FINANCE' else None,
                }
                print("Helper mapping data: ", helper_mapping_data)
                response_data, created = updateMapping_helper(helper_mapping_data, row['ID'])
                print("Response data: ", response_data)
                print("Created: ", created)

            return Response({"message": "Bulk update successful."}, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error("Unexpected error during bulk update: %s", e, exc_info=True)
            return Response(
                {"error": "An unexpected error occurred."},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

def chunker(seq, size):
    """Yield successive chunks from seq of given size."""
    for pos in range(0, len(seq), size):
        yield seq[pos:pos + size]

from collections import defaultdict

class SaveMapping(APIView):
    def post(self, request, *args, **kwargs):
        # Disallow save for read-only users
        dept = request.data.get('department') if isinstance(request.data, dict) else None
        if dept and str(dept).upper() == 'READ_ONLY':
            return Response({'error': 'Read-only users cannot save mapping.'}, status=status.HTTP_403_FORBIDDEN)
        mapping_data_qs = new_product_mapping.objects.using('default').all()
        serializer = NewProductMappingSerializer(mapping_data_qs, many=True)
        
        df = pd.DataFrame(serializer.data)

        # Track the PKs belonging to each (sku, region)
        group_to_ids = defaultdict(list)
        for obj in mapping_data_qs:
            key = (obj.marketplace_sku, obj.region)
            group_to_ids[key].append(obj.id)

        grouped = df.groupby(['marketplace_sku', 'region']).last().reset_index()
        records = grouped.to_dict('records')

        def is_blank(val):
            return val is None or str(val).strip() == ""

        rows_upserted = 0
        rows_skipped = 0
        rows_failed = 0

        # DELETE statement for "unmapping"
        delete_sql = """
            DELETE FROM look_product_hierarchy
            WHERE marketplace_sku = %s
              AND region = %s
        """

        upsert_sql = """
        IF EXISTS (
            SELECT 1 
            FROM look_product_hierarchy
            WHERE marketplace_sku = %s 
              AND region = %s
        )
        BEGIN
            UPDATE look_product_hierarchy
            SET
                asin = %s,
                im_sku = %s,
                parent_sku = %s,
                region = %s,
                marketplace = %s,
                level_1 = %s,
                level_2 = %s,
                level_3 = %s,
                level_4 = %s,
                level_5 = %s,
                company = %s,
                marketplace_sales_table = %s,
                channel = %s,
                linworks_title = %s
            WHERE marketplace_sku = %s
              AND region = %s;
        END
        ELSE
        BEGIN
            INSERT INTO look_product_hierarchy
            (
                marketplace_sku,
                asin,
                im_sku,
                parent_sku,
                region,
                marketplace,
                level_1,
                level_2,
                level_3,
                level_4,
                level_5,
                company,
                marketplace_sales_table,
                channel,
                linworks_title
            )
            VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        END
        """

        # Add SQL to update parent_sku for all records with same im_sku
        update_parent_sku_sql = """
            UPDATE look_product_hierarchy
            SET parent_sku = %s
            WHERE im_sku = %s
        """

        for row in records:
            if not isinstance(row, dict):
                continue

            marketplace_sku = row.get('marketplace_sku')
            region = row.get('region')
            asin   = row.get('asin')
            im_sku = row.get('im_sku')  # might be blank
            parent_sku = row.get('parent_sku')
            marketplace = row.get('marketplace')
            level_1     = row.get('level_1')
            level_2     = row.get('level_2')
            level_3     = row.get('level_3')
            level_4     = row.get('level_4')
            level_5     = row.get('level_5')
            company     = row.get('company', "RDX")
            sales_table = row.get('marketplace_sales_table', "stg_tr_amazon_raw")
            channel     = row.get('channel', 'Amazon')
            linworks_title = row.get('linworks_title')

            # If these critical fields are missing, skip entirely
            if (
                is_blank(marketplace_sku) or
                is_blank(region) or
                is_blank(asin)
            ):
                rows_skipped += 1
                continue

            # ----------------------------------------
            #  If IM SKU is blank => "Unmap" / Delete
            # ----------------------------------------
            if is_blank(im_sku):
                # Delete from look_product_hierarchy_test for (sku, region)
                try:
                    with transaction.atomic(using='tertiary'):
                        with connections['tertiary'].cursor() as cursor:
                            cursor.execute(delete_sql, [marketplace_sku, region])
                    
                    rows_upserted += 1

                    # Also remove the original records from new_product_mapping
                    key = (marketplace_sku, region)
                    record_ids = group_to_ids.get(key, [])
                    if record_ids:
                        new_product_mapping.objects.using('default').filter(id__in=record_ids).delete()

                except Exception as e:
                    rows_failed += 1
                    logger.error(
                        "Error deleting (sku=%s, region=%s): %s", 
                        marketplace_sku, region, e, exc_info=True
                    )
                continue

            # ----------------------------------------
            #  Otherwise, do normal upsert
            # ----------------------------------------
            try:
                with transaction.atomic(using='tertiary'):
                    with connections['tertiary'].cursor() as cursor:
                        # First, update parent_sku for all records with same im_sku
                        if not is_blank(im_sku) and not is_blank(parent_sku):
                            cursor.execute(update_parent_sku_sql, [parent_sku, im_sku])
                            logger.info(
                                "Updated parent_sku=%s for all records with im_sku=%s",
                                parent_sku, im_sku
                            )

                        # Then do the normal upsert
                        params = (
                            marketplace_sku, region, 
                            # update fields
                            asin, im_sku, parent_sku, region, marketplace,
                            level_1, level_2, level_3, level_4, level_5,
                            company, sales_table, channel, linworks_title,
                            # update WHERE
                            marketplace_sku, region,
                            # insert values
                            marketplace_sku, asin, im_sku, parent_sku, region,
                            marketplace, level_1, level_2, level_3,
                            level_4, level_5, company, sales_table,
                            channel, linworks_title
                        )
                        cursor.execute(upsert_sql, params)

                rows_upserted += 1

                # Now delete from new_product_mapping
                key = (marketplace_sku, region)
                record_ids = group_to_ids.get(key, [])
                if record_ids:
                    new_product_mapping.objects.using('default').filter(id__in=record_ids).delete()

            except Exception as e:
                rows_failed += 1
                logger.error(
                    "Error upserting group (sku=%s, region=%s): %s", 
                    marketplace_sku, region, e, exc_info=True
                )

        # Final response
        return Response({
            "message": "Finished processing groups.",
            "timestamp": datetime.now().strftime("%b %d, %Y %I:%M %p"),  # Human-readable format
            "rows_upserted_or_unmapped": rows_upserted,
            "rows_skipped_due_to_missing_fields": rows_skipped,
            "rows_failed_upsert": rows_failed
        }, status=status.HTTP_200_OK)








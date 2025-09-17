# etl/ingest_census.py
import os
import logging
from datetime import datetime
from etl.utils import safe_request, get_db_conn

CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")

# Kansas City area counties and their FIPS codes
KC_COUNTIES = {
    "Jackson County, MO": {"state": "29", "county": "095"},
    "Clay County, MO": {"state": "29", "county": "047"},
    "Platte County, MO": {"state": "29", "county": "165"},
    "Cass County, MO": {"state": "29", "county": "037"},
    "Johnson County, KS": {"state": "20", "county": "091"},
    "Wyandotte County, KS": {"state": "20", "county": "209"},
    "Leavenworth County, KS": {"state": "20", "county": "103"},
}

# Census variables we want to collect
CENSUS_VARIABLES = {
    # Population
    "B01003_001E": "total_population",
    # Age demographics
    "B01001_007E": "male_20_24",
    "B01001_008E": "male_25_29",
    "B01001_009E": "male_30_34",
    "B01001_010E": "male_35_39",
    "B01001_031E": "female_20_24",
    "B01001_032E": "female_25_29",
    "B01001_033E": "female_30_34",
    "B01001_034E": "female_35_39",
    # Income
    "B19013_001E": "median_household_income",
    "B19301_001E": "per_capita_income",
    # Education
    "B15003_022E": "bachelors_degree",
    "B15003_023E": "masters_degree",
    "B15003_024E": "professional_degree",
    "B15003_025E": "doctorate_degree",
    "B15003_001E": "total_education_pop",
    # Occupation
    "C24010_003E": "management_business_science_arts_male",
    "C24010_039E": "management_business_science_arts_female",
    "C24010_001E": "total_occupation_pop",
    # Housing
    "B25077_001E": "median_home_value",
    "B25064_001E": "median_gross_rent",
}


def fetch_census_data_for_county(state_code, county_code, year=2022):
    """
    Fetch census data for a specific county using ACS 5-Year estimates

    Args:
        state_code (str): State FIPS code
        county_code (str): County FIPS code
        year (int): Census year (default 2022 for latest ACS 5-year)

    Returns:
        dict: Census API response
    """
    if not CENSUS_API_KEY:
        logging.error("CENSUS_API_KEY not set - cannot fetch census data")
        raise ValueError("CENSUS_API_KEY is required for census data")

    # Build variables string
    variables = ",".join(CENSUS_VARIABLES.keys())

    # ACS 5-Year estimates endpoint
    url = f"https://api.census.gov/data/{year}/acs/acs5"

    params = {
        "get": variables,
        "for": f"tract:*",
        "in": f"state:{state_code} county:{county_code}",
        "key": CENSUS_API_KEY,
    }

    logging.info(f"Fetching census data for state {state_code}, county {county_code}")
    return safe_request(url, params=params)


def process_census_response(response_data, state_code, county_code):
    """
    Process raw census API response into structured format

    Args:
        response_data (list): Raw census API response
        state_code (str): State FIPS code
        county_code (str): County FIPS code

    Returns:
        list: Processed census records
    """
    if not response_data or len(response_data) < 2:
        logging.warning("No census data returned")
        return []

    # First row contains headers
    headers = response_data[0]
    data_rows = response_data[1:]

    processed_records = []

    for row in data_rows:
        # Create tract ID
        state = row[headers.index("state")]
        county = row[headers.index("county")]
        tract = row[headers.index("tract")]
        tract_id = f"{state}{county}{tract}"

        # Extract data values
        record = {"tract_id": tract_id}

        for i, header in enumerate(headers):
            if header in CENSUS_VARIABLES:
                var_name = CENSUS_VARIABLES[header]
                value = row[i]

                # Convert to numeric, handle null values
                try:
                    record[var_name] = (
                        float(value) if value not in [None, "", "-666666666"] else None
                    )
                except (ValueError, TypeError):
                    record[var_name] = None

        # Calculate derived metrics
        record = calculate_derived_metrics(record)
        processed_records.append(record)

    return processed_records


def calculate_derived_metrics(record):
    """
    Calculate derived demographic metrics from raw census data

    Args:
        record (dict): Raw census record

    Returns:
        dict: Record with derived metrics added
    """
    # Calculate age percentages (20-40 age group)
    total_pop = record.get("total_population", 0)
    if total_pop and total_pop > 0:
        age_20_40_count = sum(
            [
                record.get("male_20_24", 0) or 0,
                record.get("male_25_29", 0) or 0,
                record.get("male_30_34", 0) or 0,
                record.get("male_35_39", 0) or 0,
                record.get("female_20_24", 0) or 0,
                record.get("female_25_29", 0) or 0,
                record.get("female_30_34", 0) or 0,
                record.get("female_35_39", 0) or 0,
            ]
        )
        record["pct_age_20_40"] = (age_20_40_count / total_pop) * 100

        # Individual age group percentages
        age_20_30_count = sum(
            [
                record.get("male_20_24", 0) or 0,
                record.get("male_25_29", 0) or 0,
                record.get("female_20_24", 0) or 0,
                record.get("female_25_29", 0) or 0,
            ]
        )
        record["pct_age_20_30"] = (age_20_30_count / total_pop) * 100

        age_30_40_count = sum(
            [
                record.get("male_30_34", 0) or 0,
                record.get("male_35_39", 0) or 0,
                record.get("female_30_34", 0) or 0,
                record.get("female_35_39", 0) or 0,
            ]
        )
        record["pct_age_30_40"] = (age_30_40_count / total_pop) * 100
    else:
        record["pct_age_20_40"] = None
        record["pct_age_20_30"] = None
        record["pct_age_30_40"] = None

    # Calculate education percentages
    total_edu_pop = record.get("total_education_pop", 0)
    if total_edu_pop and total_edu_pop > 0:
        bachelors_count = record.get("bachelors_degree", 0) or 0
        record["pct_bachelors"] = (bachelors_count / total_edu_pop) * 100

        graduate_count = sum(
            [
                record.get("masters_degree", 0) or 0,
                record.get("professional_degree", 0) or 0,
                record.get("doctorate_degree", 0) or 0,
            ]
        )
        record["pct_graduate"] = (graduate_count / total_edu_pop) * 100
    else:
        record["pct_bachelors"] = None
        record["pct_graduate"] = None

    # Calculate professional occupation percentage
    total_occ_pop = record.get("total_occupation_pop", 0)
    if total_occ_pop and total_occ_pop > 0:
        professional_count = sum(
            [
                record.get("management_business_science_arts_male", 0) or 0,
                record.get("management_business_science_arts_female", 0) or 0,
            ]
        )
        record["pct_professional_occupation"] = (
            professional_count / total_occ_pop
        ) * 100
    else:
        record["pct_professional_occupation"] = None

    # Calculate population density (will need tract area for accurate calculation)
    # For now, we'll set this to None and calculate later if needed
    record["population_density"] = None

    return record


def upsert_census_data_to_db(census_records):
    """
    Insert or update census data in the database

    Args:
        census_records (list): List of processed census records
    """
    if not census_records:
        logging.info("No census data to insert")
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        inserted_count = 0
        for record in census_records:
            cur.execute(
                """
                INSERT INTO demographics (
                    tract_id, median_income, pct_bachelors, pct_graduate,
                    pct_age_20_30, pct_age_30_40, pct_age_20_40, population,
                    population_density, pct_professional_occupation, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (tract_id) DO UPDATE SET
                    median_income = EXCLUDED.median_income,
                    pct_bachelors = EXCLUDED.pct_bachelors,
                    pct_graduate = EXCLUDED.pct_graduate,
                    pct_age_20_30 = EXCLUDED.pct_age_20_30,
                    pct_age_30_40 = EXCLUDED.pct_age_30_40,
                    pct_age_20_40 = EXCLUDED.pct_age_20_40,
                    population = EXCLUDED.population,
                    population_density = EXCLUDED.population_density,
                    pct_professional_occupation = EXCLUDED.pct_professional_occupation,
                    updated_at = EXCLUDED.updated_at
            """,
                (
                    record["tract_id"],
                    record.get("median_household_income"),
                    record.get("pct_bachelors"),
                    record.get("pct_graduate"),
                    record.get("pct_age_20_30"),
                    record.get("pct_age_30_40"),
                    record.get("pct_age_20_40"),
                    record.get("total_population"),
                    record.get("population_density"),
                    record.get("pct_professional_occupation"),
                    datetime.now(),
                ),
            )
            inserted_count += 1

        conn.commit()
        logging.info(f"Successfully upserted {inserted_count} census records")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error upserting census data: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def ingest_census_data_for_kc_area():
    """
    Main function to ingest census data for Kansas City metropolitan area
    """
    total_records = 0

    for county_name, county_info in KC_COUNTIES.items():
        try:
            logging.info(f"Processing census data for {county_name}")

            # Fetch raw census data
            response_data = fetch_census_data_for_county(
                county_info["state"], county_info["county"]
            )

            if response_data:
                # Process the response
                processed_records = process_census_response(
                    response_data, county_info["state"], county_info["county"]
                )

                if processed_records:
                    # Save to database
                    upsert_census_data_to_db(processed_records)
                    total_records += len(processed_records)
                    logging.info(
                        f"Processed {len(processed_records)} tracts for {county_name}"
                    )
                else:
                    logging.warning(f"No processed records for {county_name}")
            else:
                logging.warning(f"No raw data returned for {county_name}")

        except Exception as e:
            logging.error(f"Error processing {county_name}: {e}")
            continue

    logging.info(f"Census data ingestion completed. Total records: {total_records}")
    return total_records


def main():
    """Main execution function for testing"""
    if not CENSUS_API_KEY:
        print("Error: CENSUS_API_KEY environment variable not set")
        print("Get a free API key from: https://api.census.gov/data/key_signup.html")
        return

    try:
        print("Fetching census data for Kansas City metropolitan area...")
        total_records = ingest_census_data_for_kc_area()
        print(f"Successfully processed {total_records} census tract records")

    except Exception as e:
        print(f"Error fetching census data: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()

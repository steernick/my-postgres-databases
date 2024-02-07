from sqlalchemy import create_engine, text, SMALLINT, BIGINT, NUMERIC
import pandas as pd

# Create PostgreSQL engine
password = input('Wpisz hasło do bazy danych: ')
engine = create_engine(f'postgresql://Robert:{password}@localhost:5432/demography')

# Loading xlsx file with demographic data
df = pd.read_excel('input-data/WPP2022_GEN_F01_DEMOGRAPHIC_INDICATORS_REV1.xlsx', header=16)

# cast numeric data type for columns wth numeric values
df.iloc[:, 10:] = df.iloc[:, 10:].apply(pd.to_numeric, errors='coerce')

# Adding country_id column (for future primary key)
df.sort_values('Region, subregion, country or area *', inplace=True)
df['country_id'] = df.groupby('ISO3 Alpha-code').ngroup() + 1
df = df.drop_duplicates(ignore_index=True)
df = df.dropna(subset=['country_id', 'Year']).copy()
df[['country_id', 'Year']] = df[['country_id', 'Year']].astype('int16')

# Connect to the database using a context manager to create column "countries"
with engine.begin() as connection:
    # add primary key, create sequence, set serial id in country_id
    connection.execute(text("""CREATE TABLE IF NOT EXISTS countries (
                                country_id SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                country_name VARCHAR,
                                country_iso_3_code VARCHAR,
                                country_iso_2_code VARCHAR
                                );"""))

# DataFrame with countries data
df_countries = df.iloc[:, [-1, 2, 5, 6]]
df_countries = df_countries.drop_duplicates(ignore_index=True)

df_countries.columns = ['country_id', 'country_name', 'country_iso_3_code', 'country_iso_2_code']
df_countries.sort_values('country_id', inplace=True)

# Creating table 'countries' and inserting data from DataFrame
try:
    df_countries.iloc[:, 1:].to_sql(name='countries', con=engine, if_exists='append', index=False)
except ValueError:
    print('Table "countries" already exists')

# DataFrame with country_id and population data
df_population = df.iloc[:, [-1, 10, 11, 12, 13, 14, 15, 17]]

df_population.columns = ['country_id', 'year', 'total_population_1_jan', 'total_population_1_jul',
                         'male_population_1_jul', 'female_population_1_jul', 'population_density_1_jul',
                         'median_age_1_jul']

# df_population = df_population.apply(pd.to_numeric, errors='coerce')
df_population.iloc[:, 2:6] = df_population.iloc[:, 2:6] * 1000

# Creating table 'population' and inserting data from DataFrame
try:
    df_population.to_sql(name='population', con=engine, if_exists='replace', index=False)
except ValueError:
    print('Table "population" already exists')

# DataFrame with country_id and fertility data
df_fertility = df.iloc[:, [-1, 10, 23, 24, 25, 26, 27, 28, 29]]

df_fertility.columns = ['country_id', 'year', 'births', 'birth_by_women_aged_15_to_19', 'crude_birth_rate',
                        'total_fertility_rate', 'net_reproduction_rate', 'mean_age_childbearing',
                        'sex_ratio_at_birth']

df_fertility.iloc[:, 2:4] = df_fertility.iloc[:, 2:4] * 1000

# Creating table 'fertility' and inserting data from DataFrame
try:
    df_fertility.to_sql(name='fertility', con=engine, if_exists='replace', index=False)
except ValueError:
    print('Table "fertility" already exists')

# DataFrame with country_id and mortality data
df_mortality = pd.concat([df.iloc[:, [-1, 10]], df.iloc[:, 30:63]], axis=1)

df_mortality.columns = [
    'country_id', 'year', 'total_deaths', 'male_deaths', 'female_deaths', 'crude_death_rate', 'life_expectancy_at_birth',
    'male_life_expectancy_at_birth', 'female_life_expectancy_at_birth', 'life_expectancy_at_age_15',
    'male_life_expectancy_at_age_15', 'female_life_expectancy_at_age_15', 'life_expectancy_at_age_65',
    'male_life_expectancy_at_age_65', 'female_life_expectancy_at_age_65', 'life_expectancy_at_age_80',
    'male_life_expectancy_at_age_80', 'female_life_expectancy_at_age_80', 'infant_deaths_under_age_1',
    'infant_mortality_rate', 'live_births_surviving_to_age_1', 'under_age_5_deaths','under_age_5_mortality',
    'mortality_under_age_40', 'male_mortality_under_age_40', 'female_mortality_under_age_40', 'mortality_under_age_60',
    'male_mortality_under_age_60', 'female_mortality_under_age_60', 'mortality_between_age_15_and_50',
    'male_mortality_between_age_15_and_50', 'female_mortality_between_age_15_and_50', 'mortality_between_age_15_and_60',
    'male_mortality_between_age_15_and_60', 'female_mortality_between_age_15_and_60'
]

df_mortality.iloc[:, [2, 3, 4, 18, 20, 21]] = df_mortality.iloc[:, [2, 3, 4, 18, 20, 21]] * 1000

# Creating table 'mortality' and inserting data from DataFrame
try:
    df_mortality.to_sql(name='mortality', con=engine, if_exists='replace', index=False)
except ValueError:
    print('Table "mortality" already exists')

# DataFrame with country_id and migration data
df_migration = df.iloc[:, [-1, 10, -3, -2]]

df_migration.columns = ['country_id', 'year', 'net_number_of_migrants', 'net_migration_rate']

df_migration.iloc[:, 2:3] = df_migration.iloc[:, 2:3] * 1000

# Creating table 'migration' and inserting data from DataFrame
try:
    df_migration.to_sql(name='migration', con=engine, if_exists='replace', index=False)
except ValueError:
    print('Table "migration" already exists')

# Connect to the database using a context manager to edit columns data types, add constraints etc.
with engine.begin() as connection:
    # add foreign keys, create sequence, set serial id in country_id
    connection.execute(text("""
                            ALTER TABLE fertility
                            DROP CONSTRAINT IF EXISTS fertility_countries_fkey,
                            ADD CONSTRAINT fertility_countries_fkey FOREIGN KEY (country_id)
                            REFERENCES countries (country_id),
                            ALTER COLUMN births TYPE BIGINT,
                            ALTER COLUMN birth_by_women_aged_15_to_19 TYPE BIGINT;
                            """))
    connection.execute(text("""
                            ALTER TABLE mortality
                            DROP CONSTRAINT IF EXISTS mortality_countries_fkey,
                            ADD CONSTRAINT mortality_countries_fkey FOREIGN KEY (country_id)
                            REFERENCES countries (country_id),
                            ALTER COLUMN total_deaths TYPE BIGINT,
                            ALTER COLUMN male_deaths TYPE BIGINT,
                            ALTER COLUMN female_deaths TYPE BIGINT,
                            ALTER COLUMN infant_deaths_under_age_1 TYPE BIGINT,
                            ALTER COLUMN live_births_surviving_to_age_1 TYPE BIGINT,
                            ALTER COLUMN under_age_5_deaths TYPE BIGINT;
                            """))
    connection.execute(text("""
                            ALTER TABLE population
                            DROP CONSTRAINT IF EXISTS population_countries_fkey,
                            ADD CONSTRAINT population_countries_fkey FOREIGN KEY (country_id)
                            REFERENCES countries (country_id),
                            ALTER COLUMN total_population_1_jan TYPE BIGINT,
                            ALTER COLUMN total_population_1_jul TYPE BIGINT,
                            ALTER COLUMN male_population_1_jul TYPE BIGINT,
                            ALTER COLUMN female_population_1_jul TYPE BIGINT;
                            """))
    connection.execute(text("""
                            ALTER TABLE migration
                            DROP CONSTRAINT IF EXISTS migration_countries_fkey,
                            ADD CONSTRAINT migration_countries_fkey FOREIGN KEY (country_id)
                            REFERENCES countries (country_id),
                            ALTER COLUMN net_number_of_migrants TYPE BIGINT;
                            """))

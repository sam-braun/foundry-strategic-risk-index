from pyspark.sql import DataFrame, functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("ri.foundry.main.dataset.XXXXXXXX-XXXX-XXXX-XXXX-d0e3823c7ec9"),
    qcew=Input("ri.foundry.main.dataset.XXXXXXXX-XXXX-XXXX-XXXX-8662028599db"),
    crosswalk=Input(
        "ri.foundry.main.dataset.XXXXXXXX-XXXX-XXXX-XXXX-047ee7fba876"),
)
def compute(qcew: DataFrame, crosswalk: DataFrame) -> DataFrame:
    # clean QCEW table
    df = (
        qcew.withColumn("year_int", F.col("year").cast("int"))
        .withColumn(
            "annual_avg_emplvl_num",
            F.regexp_replace(F.col("annual_avg_emplvl"),
                             ",", "").cast("double"),
        )
        .withColumn("industry_code_3", F.substring(F.col("industry_code"), 1, 3))
        .withColumn("area_fips_std", F.lpad(F.col("area_fips"), 5, "0"))
    )

    # prepare crosswalk and drop rows without MSA assignment
    xwalk = crosswalk.select(
        F.lpad(F.col("County_Code"), 5, "0").alias("county_code"),
        F.col("MSA_Code").alias("msa_code"),
        F.col("MSA_Title").alias("msa_title"),
    ).filter(F.col("msa_code").isNotNull() & (F.col("msa_code") != ""))

    # join county-level records to parent MSA
    df_msa = df.filter(F.length(F.col("area_fips_std")) == 5).join(
        xwalk, df["area_fips_std"] == xwalk["county_code"], "inner"
    )

    # keep only six 3-digit NAICS codes that map to strategic sectors
    strategic = df_msa.filter(
        F.substring(F.col("industry_code"), 1, 3).isin(
            "211", "221", "324", "325", "334", "336"
        )
    ).select(
        "msa_code", "msa_title", "year_int", "industry_code_3", "annual_avg_emplvl_num"
    )

    # assign sector labels for unambiguous codes
    non_336 = (
        strategic.filter(F.col("industry_code_3") != "336")
        .withColumn(
            "strategic_sector",
            F.when(F.col("industry_code_3") == "334", F.lit("Semiconductors"))
            .when(F.col("industry_code_3").isin("211", "221", "324"), F.lit("Energy"))
            .when(F.col("industry_code_3") == "325", F.lit("Pharmaceuticals")),
        )
        .withColumn("sector_employment_component", F.col("annual_avg_emplvl_num"))
    )

    # NAICS 336 (transportation equipment) covers both aerospace and shipbuilding
    shipbuilding = (
        strategic.filter(F.col("industry_code_3") == "336")
        .withColumn("strategic_sector", F.lit("Shipbuilding"))
        .withColumn(
            "sector_employment_component", F.col(
                "annual_avg_emplvl_num") * F.lit(0.5)
        )
    )

    aerospace = (
        strategic.filter(F.col("industry_code_3") == "336")
        .withColumn("strategic_sector", F.lit("Aerospace"))
        .withColumn(
            "sector_employment_component", F.col(
                "annual_avg_emplvl_num") * F.lit(0.5)
        )
    )

    # combine all sectors and sum employment to MSA x year x sector
    msa_sector = (
        non_336.unionByName(shipbuilding)
        .unionByName(aerospace)
        .groupBy("msa_code", "msa_title", "year_int", "strategic_sector")
        .agg(F.sum("sector_employment_component").alias("sector_employment"))
    )

    # total MSA employment from NAICS all industries
    msa_total = (
        df_msa.filter(F.col("industry_code") == "10")  # verify this in Preview
        .groupBy("msa_code", "msa_title", "year_int")
        .agg(F.sum("annual_avg_emplvl_num").alias("msa_total_employment"))
    )

    # join totals and compute each sector share of overall MSA employment
    return (
        msa_sector.join(
            msa_total, ["msa_code", "msa_title", "year_int"], "left")
        .withColumn(
            "strategic_employment_share",
            F.when(
                F.col("msa_total_employment") > 0,
                F.col("sector_employment") / F.col("msa_total_employment"),
            ),
        )
        .select(
            F.col("msa_code"),
            F.col("msa_title"),
            F.col("year_int").alias("year"),
            F.col("strategic_sector"),
            F.col("sector_employment"),
            F.col("msa_total_employment"),
            F.col("strategic_employment_share"),
        )
    )

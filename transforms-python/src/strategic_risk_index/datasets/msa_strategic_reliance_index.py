from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("ri.foundry.main.dataset.XXXXXXXX-XXXX-XXXX-XXXX-3ee30fdaabd3"),
    msa_sector=Input(
        "ri.foundry.main.dataset.XXXXXXXX-XXXX-XXXX-XXXX-d0e3823c7ec9"),
    qcew=Input("ri.foundry.main.dataset.XXXXXXXX-XXXX-XXXX-XXXX-8662028599db"),
    crosswalk=Input(
        "ri.foundry.main.dataset.XXXXXXXX-XXXX-XXXX-XXXX-047ee7fba876"),
)
def compute(msa_sector: DataFrame, qcew: DataFrame, crosswalk: DataFrame) -> DataFrame:
    """
    Roll up sector-level rows from employment transform to one row per MSA × year.
    sc_raw captures single-sector dependence (max share across all strategic sectors).
    ses_raw is share of total MSA employment in strategic industries overall.
    """
    strategic_base = (
        msa_sector.groupBy("msa_code", "msa_title",
                           "year", "msa_total_employment")
        .agg(
            F.sum("sector_employment").alias("total_strategic_employment"),
            F.max("strategic_employment_share").alias("sc_raw"),
        )
        .withColumn(
            "ses_raw",
            F.when(
                F.col("msa_total_employment") > 0,
                F.col("total_strategic_employment") /
                F.col("msa_total_employment"),
            ),
        )
        .withColumn("year", F.col("year").cast("int"))
    )

    """
    Prepare county-to-MSA crosswalk and clean the raw QCEW table for the
    HHI computation. Rows without an MSA assignment are dropped.
    """
    xwalk = crosswalk.select(
        F.lpad(F.col("County_Code"), 5, "0").alias("county_code"),
        F.col("MSA_Code").alias("msa_code"),
        F.col("MSA_Title").alias("msa_title"),
    ).filter(F.col("msa_code").isNotNull() & (F.col("msa_code") != ""))
    q = (
        qcew.withColumn("year_int", F.col("year").cast("int"))
        .withColumn(
            "emplvl_num",
            F.regexp_replace(F.col("annual_avg_emplvl"),
                             ",", "").cast("double"),
        )
        .withColumn("area_fips_std", F.lpad(F.col("area_fips"), 5, "0"))
        .filter(F.col("emplvl_num").isNotNull())
        .filter(F.length(F.col("area_fips_std")) == 5)
    )

    """
    Join QCEW county rows to their parent MSA. Explicit column selection avoids
    ambiguous duplicates since q has no msa_code/msa_title columns.
    """
    q_msa = q.join(xwalk, q["area_fips_std"] == xwalk["county_code"], "inner").select(
        q["area_fips_std"],
        q["industry_code"],
        q["own_code"],
        q["emplvl_num"],
        q["year_int"],
        xwalk["msa_code"],
        xwalk["msa_title"],
    )

    """
    Compute HHI-based economic fragility. Only 3-digit NAICS rows are used, 
    county rows are aggregated to the MSA level first so each industry is counted
    once per metro. fragility_raw = \sigma(industry_share^2).
    """
    msa_industry = (
        q_msa.filter(F.col("industry_code").rlike("^[0-9]{3}$"))
        .groupBy("msa_code", "msa_title", "year_int", "industry_code")
        .agg(F.sum("emplvl_num").alias("industry_employment"))
        .withColumn("year", F.col("year_int").cast("int"))
        .drop("year_int")
    )

    msa_totals_for_hhi = msa_industry.groupBy("msa_code", "msa_title", "year").agg(
        F.sum("industry_employment").alias("msa_industry_total_employment")
    )

    fragility = (
        msa_industry.join(
            msa_totals_for_hhi, ["msa_code", "msa_title", "year"], "inner"
        )
        .withColumn(
            "industry_share",
            F.when(
                F.col("msa_industry_total_employment") > 0,
                F.col("industry_employment") /
                F.col("msa_industry_total_employment"),
            ),
        )
        .withColumn(
            "industry_share_sq", F.col(
                "industry_share") * F.col("industry_share")
        )
        .groupBy("msa_code", "msa_title", "year")
        .agg(F.sum("industry_share_sq").alias("fragility_raw"))
    )

    """
    Convert each raw metric to within-year percentile rank (0–100), then
    combine into final SRI: SRI = (ses_pct + sc_pct + fragility_pct) / 3.
    """
    base = strategic_base.join(
        fragility, ["msa_code", "msa_title", "year"], "inner")

    w_ses = Window.partitionBy("year").orderBy(F.col("ses_raw"))
    w_sc = Window.partitionBy("year").orderBy(F.col("sc_raw"))
    w_frag = Window.partitionBy("year").orderBy(F.col("fragility_raw"))

    return (
        base.withColumn("ses_pct", F.percent_rank().over(w_ses) * F.lit(100.0))
        .withColumn("sc_pct", F.percent_rank().over(w_sc) * F.lit(100.0))
        .withColumn("fragility_pct", F.percent_rank().over(w_frag) * F.lit(100.0))
        .withColumn(
            "sri",
            (F.col("ses_pct") + F.col("sc_pct") +
             F.col("fragility_pct")) / F.lit(3.0),
        )
        .select(
            "msa_code",
            "msa_title",
            "year",
            "total_strategic_employment",
            "msa_total_employment",
            "ses_raw",
            "sc_raw",
            "fragility_raw",
            "ses_pct",
            "sc_pct",
            "fragility_pct",
            "sri",
        )
    )

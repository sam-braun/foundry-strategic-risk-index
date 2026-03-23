# Strategic U.S. Industry Dependency & Shock Risk Index

A Palantir Foundry pipeline that ranks U.S. metro areas by their economic exposure to disruptions in nationally strategic industries of semiconductors, energy, pharmaceuticals, aerospace, and shipbuilding.

Built with PySpark transforms on top of BLS QCEW employment data covering 935 metro and micropolitan statistical areas. To learn more and read about my process and analysis for this work, please visit my [portfolio site](https://sam-braun.github.io/projects/geo_strategic_risk)!

## Pipeline

The pipeline consists of two chained Foundry transforms:

- Transform 1: `msa_strategic_sector_employment`
  - Maps [NAICS codes](https://www.census.gov/naics/) to strategic sectors
  - Aggregates employment by MSA x Year x Sector
  - Computes each sector's share of metro total employment
- Transform 2: `msa_strategic_reliance_index`
  - Computes strategic employment share (`SES_pct`)
  - Computes sector concentration by max single-sector share (`SC_pct`)
  - Computes economic fragility via [Herfindahl-Hirschman index](https://www.investopedia.com/terms/h/hhi.asp) (`Fragility_pct`)
  - Percentile-ranks each metric within year
  - Combines into final strategic shock risk index

### NAICS Sector Mapping

| Strategic Sector | NAICS | Description |
|---|---|---|
| Semiconductors | 334 | Computer & Electronic Product Manufacturing |
| Energy | 211, 221, 324 | Oil & Gas Extraction, Utilities, Petroleum Mfg |
| Pharmaceuticals | 325 | Chemical Manufacturing |
| Aerospace | 336 | Transportation Equipment Mfg |
| Shipbuilding | 336 | Transportation Equipment Mfg |

### SRI Formula

Ranking in the strategic risk index is calculated using:

```
SRI = mean(SES_pct, SC_pct, Fragility_pct)
```

Where each component is percentile-ranked across all metros within a given year (0–100 scale):

- `SES_pct`: Strategic employment share: strategic-sector jobs as a share of total metro employment
- `SC_pct`: Sector concentration: the largest single strategic sector's share of total metro employment (higher = more concentrated in one sector)
- `Fragility_pct`: Economic fragility: a Herfindahl-Hirschman index over all industries, measuring overall economic diversification

### Output

| Transform | Dataset | Description |
|---|---|---|
| 1 | `int_msa_strategic_sector_employment.csv` | Employment by MSA x year x strategic sector |
| 2 | `stg_sri_index.csv` | Final SRI scores and component percentiles by MSA x year |

I included copies of the intermediate and staged tables in `transforms-python/data`, but they are not automatically saved there by Foundry and are instead generated as their own external dataset objects. The process for output generation is described in [Getting Started](#getting-started).

## Data Sources

- Bureau of Labor Statistics Quarterly Census of Employment and Wages (QCEW): https://www.bls.gov/cew/downloadable-data-files.htm
- Census CBSA definitions and county-to-MSA crosswalk: https://www.census.gov/programs-surveys/metro-micro.html

## Project Structure

The core logic implemented for the transforms can be found:

```
transforms-python/src/strategic_risk_index/
├── datasets/
│   ├── msa_strategic_sector_employment.py   # transform 1
│   └── msa_strategic_reliance_index.py      # transform 2
└── pipeline.py
```

## Getting Started

1. Create a [Palantir Foundry dev account](https://www.palantir.com/docs/foundry/getting-started/overview).
2. Upload QCEW and Census CBSA source datasets to Foundry and save as dataset objects. Save their `RID` (Palantir resource identifier) values.
3. Initialize dataset objects for the intermediate and staged output tables (respective outputs of transforms 1 and 2). Save their `RID` values.
4. Plug dataset object `RID` values into the `@transform_df` decorators at the top of each transform file. 
5. Initialize transform builds in order of [pipeline](#pipeline) and execute build on each transform.
6. Verify each build executes successfully and intermediate and staged output tables are populated.
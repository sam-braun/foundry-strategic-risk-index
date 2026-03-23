from transforms.api import Pipeline

from strategic_risk_index import datasets


sri_pipeline = Pipeline()
sri_pipeline.discover_transforms(datasets)

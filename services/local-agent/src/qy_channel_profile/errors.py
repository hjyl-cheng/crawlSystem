class ProfileProcessingError(Exception):
    """Base error for a rejected analysis request."""

    code = "PROFILE_PROCESSING_ERROR"


class ContractError(ProfileProcessingError):
    code = "OUTPUT_CONTRACT_INVALID"


class SnapshotError(ProfileProcessingError):
    code = "SNAPSHOT_NOT_FOUND"


class PriorCatalogError(ProfileProcessingError):
    code = "PRIOR_CATALOG_INVALID"


class ModelBundleError(ProfileProcessingError):
    code = "MODEL_BUNDLE_INVALID"


class FeatureStoreError(ProfileProcessingError):
    code = "FEATURE_STORE_INVALID"


class TrainingDataError(ProfileProcessingError):
    code = "TRAINING_DATA_INVALID"

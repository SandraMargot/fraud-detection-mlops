from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.feature_extraction import FeatureHasher


class HashingEncoder(BaseEstimator, TransformerMixin):
    def __init__(self, n_features=128):
        self.n_features = n_features
        self.hasher = FeatureHasher(
            n_features=n_features,
            input_type="string"
        )

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return self.hasher.transform(
            X.astype(str).values
        )
